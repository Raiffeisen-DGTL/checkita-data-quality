package ru.raiffeisen.checkita.storage

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.types.{StructField, StructType}
import ru.raiffeisen.checkita.config.Enums.{DQStorageType, TrendCheckRule}
import ru.raiffeisen.checkita.storage.Connections._
import ru.raiffeisen.checkita.storage.Models._
import ru.raiffeisen.checkita.utils.Common.camelToSnakeCase
import ru.raiffeisen.checkita.utils.EnrichedDT
import slick.jdbc._

import java.sql.Timestamp
import java.util.concurrent.Executors
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.reflect.runtime.universe._
import scala.util.Try

object Managers {
  
  sealed abstract class DqStorageManager {
    protected val ds: DqStorageConnection
    val tables: Tables
    def saveErrors: Boolean = ds.config.saveErrorsToStorage

    protected def getTimeWindow(startDT: EnrichedDT,
                                windowSize: String,
                                windowOffset: Option[String]): (Timestamp, Timestamp) = {
      val window = Duration(windowSize)
      val offset = windowOffset.map(Duration(_)).getOrElse(Duration.Zero)
      val untilDT = startDT.getUtcTsWithOffset(offset)
      val fromDT = startDT.getUtcTsWithOffset(window + offset)
      (fromDT, untilDT)
    }

    protected def filterByOffset[R <: MetricResult](records: Seq[R], offset: Int): Seq[R] =
      records.sortBy(r => (-r.referenceDate.getTime, r.metricName))
        .zipWithIndex.filter(t => t._2 >= offset).map(_._1)

    def saveResults[R <: DQEntity : TypeTag](results: Seq[R])(implicit ops: tables.DQTableOps[R]): String

    def loadMetricResults[R <: MetricResult : TypeTag](jobId: String,
                                                       metricIds: Seq[String],
                                                       rule: TrendCheckRule,
                                                       startDT: EnrichedDT,
                                                       windowSize: String,
                                                       windowOffset: Option[String])
                                                      (implicit ops: tables.DQTableOps[R]): Seq[R]
    
    def getConnection: DqStorageConnection
    def close(): Unit
  }
  object DqStorageManager {
    def apply(ds: DqStorageConnection)(implicit spark: SparkSession,
                                       fs: FileSystem): DqStorageManager = ds match {
      case fileConnection: DqStorageFileConnection => new DqFileStorageManager(fileConnection, spark, fs)
      case hiveConnection: DqStorageHiveConnection => new DqHiveStorageManager(hiveConnection, spark)
      case jdbcConnection: DqStorageJdbcConnection => new DqJdbcStorageManager(jdbcConnection)
    }
  }
  
  
  sealed trait SparkBasedManager { this: DqStorageManager =>
    protected def getEncoder[R <: Product : TypeTag]: Encoder[R] = Encoders.product[R]
    protected def getSchema(e: Encoder[_]): StructType = StructType(
      e.schema.map(c => StructField(camelToSnakeCase(c.name), c.dataType, c.nullable))
    )
    
    protected def updateData[R <: DQEntity : TypeTag](spark: SparkSession,
                                                      currentData: DataFrame,
                                                      results: Seq[R],
                                                      schema: StructType): DataFrame = {
      val uniqueColumns = results.head.uniqueFieldNames

      val currentDataFmt = currentData.select(
        schema.map(c => col(c.name).cast(c.dataType)) : _*
      ).withColumn("rank", lit(2))

      val newData = spark.createDataFrame(results)
      val newDataFmt = spark.createDataFrame(newData.rdd, schema)
        .withColumn("rank", lit(1))

      require(
        currentDataFmt.schema.zip(newDataFmt.schema).forall(t => t._1 == t._2),
        "History storage schema does not match the schema of results being written."
      )

      val w = Window.partitionBy(uniqueColumns.map(col) : _*).orderBy(col("rank"))

      currentDataFmt.union(newDataFmt)
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") === 1)
        .select(schema.map(c => col(c.name)) : _*)
    }

    def loadMetricData[R <: MetricResult : TypeTag](currentData: DataFrame,
                                                    schema: StructType,
                                                    encoder: Encoder[R],
                                                    jobId: String,
                                                    metricIds: Seq[String],
                                                    rule: TrendCheckRule,
                                                    startDT: EnrichedDT,
                                                    windowSize: String,
                                                    windowOffset: Option[String],
                                                   ): Seq[R] = {
      val preDF = currentData.filter(col("job_id") === jobId)
        .filter(col("metric_id").isin(metricIds: _*))

      rule match {
        case TrendCheckRule.Record =>
          val numRows = windowSize.toInt
          val offset = windowOffset.map(_.toInt).getOrElse(0)

          val w = Window.partitionBy(col("job_id"), col("metric_id"))
            .orderBy(col("reference_date").desc, col("metric_name"))

          preDF.filter(col("reference_date") < startDT.getUtcTS)
            .withColumn("row_number", row_number.over(w))
            .filter(col("row_number") <= numRows + offset && col("row_number") > offset)
            .select(schema.map(c => col(c.name)) : _*)
            .as[R](encoder).collect().toSeq

        case TrendCheckRule.Datetime =>
          val (fromDT, untilDT) = getTimeWindow(startDT, windowSize, windowOffset)
          preDF.filter(col("reference_date") >= fromDT)
            .filter(col("reference_date") < untilDT)
            .select(schema.map(c => col(c.name)) : _*)
            .as[R](encoder).collect().toSeq
      }
    }
  }
  
  trait SlickProfile { this: DqStorageManager =>
    val dbType: DQStorageType
    
    protected lazy val profile: JdbcProfile = dbType match {
      case DQStorageType.Postgres => PostgresProfile
      case DQStorageType.Oracle => OracleProfile
      case DQStorageType.MySql => MySQLProfile
      case DQStorageType.MSSql => SQLServerProfile
      case DQStorageType.H2 => H2Profile
      case DQStorageType.SQLite => SQLiteProfile
      case _ => PostgresProfile // use postgres profile just in order to get table instances
    }
  }
  
  class DqJdbcStorageManager(val ds: DqStorageJdbcConnection) extends DqStorageManager with SlickProfile {

    val batchSize: Int = 100
    val dbType: DQStorageType = ds.getType
    import profile.api._
    
    implicit val ec: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newWorkStealingPool(4))
    private val db = Database.forDataSource(ds.getDataSource, Some(ds.getMaxConnections))
    val tables: Tables = new Tables(profile)

    private def runUpsert[R <: DQEntity : TypeTag](data: Seq[R],
                                                   ops: tables.DQTableOps[R]): Future[(String, Int)] = {
      val table = ops.getTableQuery(ds.getSchema)
      val tableName = table.baseTableRow.tableName
      
      val conflicts = db.run(
        table.filter { t =>
          data.map(t.getUniqueCond).reduceLeftOption(_ || _).getOrElse(true: Rep[Boolean])
        }.result
      )
      val upsert = conflicts.flatMap{ rows =>
        val predicate = (r: R) => rows.map(rr => rr.uniqueFields).contains(r.uniqueFields)
        val upsertQuery = DBIO.sequence(
          data.filter(predicate).map { nr =>
            table.filter(t => t.getUniqueCond(nr)).update(nr)
          } :+ (table ++= data.filter(!predicate(_)))
        )
        db.run(upsertQuery)
      }

      upsert.map{ s =>
        val numRows = s.map{
          case x: Int => x
          case Some(x) => x.asInstanceOf[Int]
          case None => 0
        }.sum
        (tableName, numRows)
      }
    }

    /**
     * Saves results to a corresponding table in storage database.
     *
     * @param results Sequence of results to be saved
     * @param ops     Implicit table extension methods used to retrieve
     *                instance of Slick table that matches the result type.
     * @tparam R Type of results to be saved
     * @return Results write operation status string.
     * @note Results are saved with use of upsert logic.
     *       Thus, existing records that conflicts with new ones by unique
     *       constraint are found first. Then, these records replaced with new ones
     *       and remaining records are appended to the table.
     *       Process of finding conflicting records involves multiple logical operators
     *       chaining that might lead to stack overflow error in Slick
     *       (see https://github.com/slick/slick/issues/1606).
     *       In order to avoid such error a sequence of results is
     *       split into batches of `batchSize` and upsert operation
     *       is performed per each batch separately.
     */
    def saveResults[R <: DQEntity : TypeTag](results: Seq[R])
                                            (implicit ops: tables.DQTableOps[R]): String =
    if (results.isEmpty) "Nothing to save." else {
      val result = results.grouped(batchSize)
        .map(r => runUpsert[R](r, ops))
        .reduceLeft((fa, fb) => fa.flatMap(a => fb.map(b => (a._1, a._2 + b._2))))

      val (table, numRows) =  Await.result(result, Duration.Inf)
      s"Successfully upsert $numRows rows into table '$table'."
    }

    def loadMetricResults[R <: MetricResult : TypeTag](jobId: String,
                                                       metricIds: Seq[String],
                                                       rule: TrendCheckRule,
                                                       startDT: EnrichedDT,
                                                       windowSize: String,
                                                       windowOffset: Option[String])
                                                      (implicit ops: tables.DQTableOps[R]): Seq[R] = {
      val table = ops.getTableQuery(ds.getSchema).asInstanceOf[TableQuery[tables.MetricResultTable[R]]]
      rule match {
        case TrendCheckRule.Record =>
          val numRows = windowSize.toInt
          val offset = windowOffset.map(_.toInt).getOrElse(0)

          val query = DBIO.sequence(
            metricIds.map{ mId =>
              table.filter(t => t.jobId === jobId && t.metricId === mId && t.referenceDate < startDT.getUtcTS)
                .sortBy(_.referenceDate.desc).take(numRows + offset).result
            }
          )
          Await.result(db.run(query).map(s => s.flatMap(ss => filterByOffset(ss, offset))), Duration.Inf)
        case TrendCheckRule.Datetime =>
          val (fromDT, untilDT) = getTimeWindow(startDT, windowSize, windowOffset)
          val query = table.filter( t =>
            t.jobId === jobId &&
              metricIds.map(mId => t.metricId === mId).reduceLeftOption(_ || _).getOrElse(true: Rep[Boolean]) &&
              t.referenceDate >= fromDT &&
              t.referenceDate < untilDT
          ).result

          Await.result(db.run(query), Duration.Inf)
      }
    }
    
    def close(): Unit = db.close()
    def getConnection: DqStorageConnection = ds
  }
  
  class DqHiveStorageManager(val ds: DqStorageHiveConnection,
                             spark: SparkSession) extends DqStorageManager with SlickProfile with SparkBasedManager {

    private val hiveSchema = ds.getSchema
    val dbType: DQStorageType = ds.getType
    val tables: Tables = new Tables(profile)

    private def getBasicVars[R <: DQEntity : TypeTag](ops: tables.DQTableOps[R]): (String, Encoder[R], StructType) = {
      val tableName = s"$hiveSchema.${ops.getTableName(ops.getTableQuery(None))}"
      val encoder = getEncoder[R]
      val schema = getSchema(encoder)
      (tableName, encoder, schema)
    }
    
    def saveResults[R <: DQEntity : TypeTag](results: Seq[R])(implicit ops: tables.DQTableOps[R]): String = {
      val (targetTable, _, schema) = getBasicVars[R](ops)
      val curJobId = results.head.jobId

      // Hive tables are partitioned by job_id and, therefore, it must be at last position in dataframe:
      val shuffledSchema = StructType(
        schema.filter(_.name != "job_id") ++ schema.filter(_.name == "job_id")
      )

      val currentData = spark.read.table(targetTable).filter(col("job_id") === curJobId)
      val resultsData = updateData(spark, currentData, results, schema)
      resultsData.select(shuffledSchema.map(c => col(c.name)) : _*)
        .repartition(1).write.mode(SaveMode.Overwrite).insertInto(targetTable)
      
      s"Successfully upsert ${results.size} rows into hive table '$targetTable'."
    }

    def loadMetricResults[R <: MetricResult : TypeTag](jobId: String,
                                                       metricIds: Seq[String],
                                                       rule: TrendCheckRule,
                                                       startDT: EnrichedDT,
                                                       windowSize: String,
                                                       windowOffset: Option[String])
                                                      (implicit ops: tables.DQTableOps[R]): Seq[R] = {

      val (targetTable, encoder, schema) = getBasicVars[R](ops)
      val currentData = Try(spark.read.table(targetTable)).toOption

      currentData.map { df =>
        loadMetricData[R](df, schema, encoder, jobId, metricIds, rule, startDT, windowSize, windowOffset)
      }.getOrElse(Seq.empty[R])
    }
    
    def close(): Unit = ()
    def getConnection: DqStorageConnection = ds
  }

  class DqFileStorageManager(val ds: DqStorageFileConnection,
                             spark: SparkSession,
                             fs: FileSystem) extends DqStorageManager with SlickProfile with SparkBasedManager {

    private val storagePath = ds.getPath
    val dbType: DQStorageType = ds.getType
    val tables: Tables = new Tables(profile)

    private def getBasicVars
    [R <: DQEntity : TypeTag](ops: tables.DQTableOps[R]): (String, String, Encoder[R], StructType) = {
      val tableName = ops.getTableName(ops.getTableQuery(None))
      val resultsDir = s"$storagePath/$tableName"
      val tmpDir = s"$storagePath/.tmp/$tableName"
      val encoder = getEncoder[R]
      val schema = getSchema(encoder)
      (resultsDir, tmpDir, encoder, schema)
    }

    def saveResults[R <: DQEntity : TypeTag](results: Seq[R])(implicit ops: tables.DQTableOps[R]): String = {
      val (resultsDir, tmpDir, _, schema) = getBasicVars[R](ops)

      val currentData = if (!fs.exists(new Path(resultsDir)))
        spark.createDataFrame(rowRDD=spark.sparkContext.emptyRDD[Row], schema=schema)
      else
        spark.read.parquet(resultsDir).select(schema.map(c => col(c.name).cast(c.dataType)) : _*)
        
      val resultsData = updateData(spark, currentData, results, schema)
      resultsData.write.mode(SaveMode.Overwrite).parquet(tmpDir)
      spark.read.parquet(tmpDir).repartition(1).write.mode(SaveMode.Overwrite).parquet(resultsDir)
      s"Successfully upsert ${results.size} rows into results folder '$resultsDir'."
    }

    def loadMetricResults[R <: MetricResult : TypeTag](jobId: String,
                                                       metricIds: Seq[String],
                                                       rule: TrendCheckRule,
                                                       startDT: EnrichedDT,
                                                       windowSize: String,
                                                       windowOffset: Option[String])
                                                      (implicit ops: tables.DQTableOps[R]): Seq[R] = {

      val (resultsDir, _, encoder, schema) = getBasicVars[R](ops)
      val currentData = Try(spark.read.parquet(resultsDir)).toOption

      currentData.map { df =>
        loadMetricData[R](df, schema, encoder, jobId, metricIds, rule, startDT, windowSize, windowOffset)
      }.getOrElse(Seq.empty[R])
    }
    
    def close(): Unit = {
      val tmpDir = new Path(s"$storagePath/.tmp")
      if (fs.exists(tmpDir)) fs.delete(tmpDir, true)
    }
    def getConnection: DqStorageConnection = ds
  }
  
}