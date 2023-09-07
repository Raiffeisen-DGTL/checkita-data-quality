package ru.raiffeisen.checkita.metrics

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.file.FileMetrics.RowCountMetricCalculator
import ru.raiffeisen.checkita.utils.{DQSettings, Logging, getParametrizedMetricTail}

import scala.collection.mutable

object MetricProcessor extends Logging {

  // Some custom types to increase readability of the code
  type ParamMap = Map[String, Any]
  type MetricErrors = Map[String, (Seq[String], mutable.Seq[Seq[String]])]

  /**
   * Processes all specified metrics for a specific dataframe
   *
   * @param df          dataframe to calculate metrics
   * @param colMetrics  list of column metrics
   * @param fileMetrics list of file metrics
   * @return two maps (for column and file metrics) in form (parametrized_name -> (result, additional result)
   *         additional result used in top N metrics, since the result of this metric look like (frequency, value)
   */
  def processAllMetrics(df: DataFrame,
                        colMetrics: Seq[ColumnMetric],
                        fileMetrics: Seq[FileMetric],
                        sourceKeyFields: Seq[String])(implicit settings: DQSettings,
                                                      sc: SparkContext,
                                                      sparkSes: SparkSession,
                                                      fs: FileSystem):
  (Map[Seq[String], Map[ColumnMetric,(Double, Option[String])]],
    Map[FileMetric, (Double, Option[String])], MetricErrors) = {

    /**
     * Calls a unified metric calculator's constructor
     *
     * @param tag      class tag of calculator
     * @param paramMap parameter map
     * @return instance of metric calculator
     */
    def initGroupCalculator(tag: Class[_], paramMap: ParamMap): MetricCalculator = {
      tag
        .getConstructor(classOf[Map[String, Any]])
        .newInstance(paramMap)
        .asInstanceOf[MetricCalculator]
    }

    /**
     * Applies parametrized metric tail to metric id
     *
     * @param metric input metric
     * @return explicit metric id
     */
    def getParametrizedMetricName(metric: Metric): String = {
      val paramTail = getParametrizedMetricTail(metric.paramMap)
      metric.name + paramTail
    }

    // init file metric calculators
    val fileMetCalculators: Map[FileMetric, MetricCalculator] =
      fileMetrics.map { mm =>
      {
        val calc = mm.name match {
          case "ROW_COUNT" => RowCountMetricCalculator(0) //return rows count
          case x           => throw IllegalParameterException(x)
        }
        mm -> calc
      }
      }.toMap

    /**
     * The main idea of all that construction is calculators grouping.
     *
     * @example You want to obtain multiple quantiles for a specific column,
     *          but calling a new instance of tdigest for each metric isn't effective.
     *
     *          To avoid that first, we're mapping all metric to their calculator classes, then we are
     *          grouping them by column and parameters.
     * @example "FIRST_QUANTILE" for column "A" with parameter "accuracyError"=0.0001
     *          will require an intance of TDigestMetricCalculator. "MEDIAN_VALUE" for column "B" with
     *          the same parameter "accuracyError"=0.0001 will also require an instance of TDigestMetricCalculator.
     *          In our approach the instance will be the same and it will return us results like
     *          Map(("MEDIAN_VALUE:..."->result1),("FIRST_QUANTILE:..."->result2),...)
     *
     *          So in the end we are initializing only unique calculators.
     */
    val metricsByColumn: Map[Seq[String], Seq[ColumnMetric]] = colMetrics.groupBy(_.columns)

    val columnsIndexes: Map[String, Int] = df.schema.fieldNames.map(s => s -> df.schema.fieldIndex(s)).toMap
    val columnsValues: Map[Int, String]  = df.schema.fieldNames.map(s => df.schema.fieldIndex(s) -> s).toMap
    val sourceKeyIds: Seq[Int]           = sourceKeyFields.flatMap(i => columnsIndexes.get(i))

    log.info(s"KEY FIELDS: [${sourceKeyFields.mkString(",")}]")

    if (sourceKeyIds.size != sourceKeyFields.size) log.error("Some of key fields were not found! Please, check them.")

    val dumpSize = settings.errorDumpSize

    val groupedCalculators: Map[Seq[String], Seq[(MetricCalculator, Seq[ColumnMetric])]] = {
      // Seq[String] === Seq[ColumnName]
      // All ColumnMetric correspond to the same MetricCalculator
      metricsByColumn.map {
        case (colId, metList) =>
          colId -> metList
            .map(mm => (mm, initGroupCalculator(MetricMapper.getMetricClass(mm.name), mm.paramMap)))
            .groupBy(_._2)
            .mapValues(_.map(_._1))
            .toSeq
      }
    }

    /**
     * To calculate metrics we are using three-step processing:
     * 1. Iterating over RDD and passing values to the calculators
     * 2. Updating partition calculators before merging (operations like trimming, shifting, etc)
     * 3. Reducing (merging partition calculator)
     *
     * File and column metrics are storing separately
     */

    val failedRowsForMetric = ErrorAccumulator(mutable.ArrayBuffer.empty[(String, String, String)])
    sc.register(failedRowsForMetric)

    val (columnMetricCalculators, fileMetricCalculators):
      (Map[Seq[String], Seq[(MetricCalculator, Seq[ColumnMetric])]], Map[FileMetric, MetricCalculator]) =
      df.rdd.treeAggregate((groupedCalculators, fileMetCalculators))(
        seqOp = {
          case ((colMetCalcs: Map[Seq[String], Seq[(MetricCalculator, Seq[ColumnMetric])]],
          fileMetCalcs: Map[FileMetric, MetricCalculator]),
          row: Row) =>
            val updatedColRes: Map[Seq[String], Seq[(MetricCalculator, Seq[ColumnMetric])]] =
              colMetCalcs.map(m => {
                val columnValues: Seq[Any] = m._1.map(columnsIndexes).map(row.get)

                val incrementedCalculators: Seq[(MetricCalculator, Seq[ColumnMetric])] =
                  colMetCalcs(m._1).map {
                    case (calc: MetricCalculator, met: Seq[ColumnMetric]) =>
                      (calc.increment(columnValues), met)
                  }

                (m._1, incrementedCalculators)
              })

            val updatedFileRes: Map[FileMetric, MetricCalculator] = fileMetCalcs
              .map(calc => calc._1 -> calc._2.increment(Seq(row)))

            // _1: collect metric id and list of columns ids, which correspond to that metric:
            // _2: collect list of all columns to report: (keyFields + metricColumns)
            val failedMetricIds: Iterable[(String, Seq[Int])] =
            updatedColRes.values.flatten.collect {
              case (ic: StatusableCalculator, met: Seq[ColumnMetric])
                if ic.getStatus == CalculatorStatus.FAILED && ic.getFailCounter <= dumpSize =>
                met.map { m =>
                  val idsWithCols = (Seq(m.id) ++ m.columns).mkString("%~%")
                  val allIds = sourceKeyIds ++ m.columns.map(columnsIndexes).filter(!sourceKeyIds.contains(_))
                  (idsWithCols, allIds)
                }
            }.flatten

            if (failedMetricIds.nonEmpty && sourceKeyIds.nonEmpty) {
              // group failed metrics by their list of columns.
              // Thus, for each list of columns there will be
              // list of metrics and list of row values for that columns.
              val groupedFailedRows = failedMetricIds
                .groupBy(_._2)
                .mapValues(x => x.map(_._1))
                .toSeq.map{ t =>
                val metIds = t._2.mkString("<;>")
                val colValues = t._1.map(id => if (row.isNullAt(id)) "" else row.get(id)).mkString("<;>")
                val colNames = t._1.map(columnsValues).mkString("<;>")
                (metIds, colNames, colValues)
              }

              groupedFailedRows.foreach(failedRowsForMetric.add)
            }

            (updatedColRes, updatedFileRes)
        },
        combOp = (r, l) => {
          val colMerged: Map[Seq[String], Seq[(MetricCalculator, Seq[ColumnMetric])]] =
            l._1.map(c => {
              val zipedCalcs: Seq[((MetricCalculator, Seq[ColumnMetric]), (MetricCalculator, Seq[ColumnMetric]))] = r
                ._1(c._1) zip l._1(c._1)
              val merged: Seq[(MetricCalculator, Seq[ColumnMetric])] =
                zipedCalcs.map(zc => (zc._1._1.merge(zc._2._1), zc._1._2))
              (c._1, merged)
            })
          val fileMerged: Map[FileMetric, MetricCalculator] =
            r._2.map(x => x._1 -> l._2(x._1).merge(x._2))
          (colMerged, fileMerged)
        }
      )

    columnMetricCalculators.values.flatten.foreach {
      case (calc: StatusableCalculator, metrics) =>
        log.info(s"For metrics:[${metrics.map(_.id).mkString(",")}] were found ${calc.getFailCounter} errors.")
      case (_, _) =>
    }

    // restructuring failedRowsForMetric:
    // group data by metric id and column ids that correspond to that metric
    // and then move column id to the values of the map while exploding list of row strings
    // into list of lists of row values.
    val accumulator: mutable.Seq[(Array[String], String, String)] =
    failedRowsForMetric.value.map {
      case (metIds, colNames, colValues) => (
        metIds.split("<;>", -1),
        colNames,
        colValues
      )
    }
    val trimmedAccumulator: MetricErrors = accumulator
      .flatMap { case (met, cols, row) => met.map(m => (m, cols) -> row) }
      .groupBy(_._1)
      .map(x => x._1._1 -> (
        x._1._2.split("<;>", -1).toSeq,
        x._2.map(_._2.split("<;>", -1).toSeq)))

    /**
     * After processing metrics, there are only results from calculators, not connected with specific
     * metric ids. To do that linking we're generating unique parametrized name of the specific metric in
     * the same way, as they were made in calculators. Then we're combining them together.
     *
     * @note If there are two identical metric, the result will be calculated only once and both metrics will be
     *       linked to it.
     * @example Same condition as in previous example: 2 quanitile metrics over one column with the same
     *          accuracy error. They are evaluated in one calculator. Calculator result is the following:
     *          Map(("MEDIAN_VALUE:..."->result1),("FIRST_QUANTILE:..."->result2),...)
     *          Now, name from calculator and exact metric's name should be the same to be connected.
     */
    // combine file metrics and results
    val fileMetResults: Map[FileMetric, (Double, Option[String])] =
    fileMetricCalculators.map(x => x._1 -> x._2.result()(x._1.name))

    // init list of all metrics per column
    val resultsMap: Map[Seq[String], Map[String, (Double, Option[String])]] =
      columnMetricCalculators.map { colres =>
        val resMap: Map[String, (Double, Option[String])] =
          colres._2.flatMap(calc => calc._1.result()).toMap
        (colres._1, resMap)
      }

    /**
     * Metrics like TOP_N and TOP_N_AGLEBIRD producing multiple results with the different names,
     * template "{metric_name}:{metric_params}_{index}" is used to link in that situation
     */
    // process metrics (SPLIT)
    val processedMetrics: Map[Seq[String], Seq[ColumnMetric]] =
    metricsByColumn.map(col => {

      def splitMetric(baseMetric: ColumnMetric, splitNum: Int): Seq[ColumnMetric] = {
        def generateMetric(bm: ColumnMetric, sn: Int, aggr: Seq[ColumnMetric]): Seq[ColumnMetric] = {
          if (sn > 0) {
            val newMetric = ColumnMetric(
              baseMetric.id + "_" + sn.toString,
              baseMetric.name + "_" + sn.toString,
              baseMetric.description,
              baseMetric.source,
              baseMetric.columns,
              baseMetric.paramMap
            )
            return generateMetric(bm, sn - 1, aggr ++ Seq(newMetric))
          }
          aggr
        }

        generateMetric(baseMetric, splitNum, Seq.empty)
      }

      val processed: Seq[ColumnMetric] = col._2.flatMap(metric =>
        metric.name match {
          case "TOP_N" =>
            splitMetric(metric, metric.paramMap.getOrElse("targetNumber", 10).toString.toInt)
          case _ => Seq(metric)
        })
      (col._1, processed)
    })

    // combine column metrics and results
    val unitedMetricResult: Map[Seq[String], Map[ColumnMetric, (Double, Option[String])]] =
      processedMetrics.map(colmet => {
        val resMap: Map[String, (Double, Option[String])] =
          resultsMap(colmet._1)
        val metResMap: Map[ColumnMetric, (Double, Option[String])] = colmet._2
          .map(met => {
            (met, resMap.getOrElse(getParametrizedMetricName(met), (0.0, Some("not_present"))))
          })
          .toMap
        (colmet._1, metResMap)
      })

    (unitedMetricResult, fileMetResults, trimmedAccumulator)
  }
}