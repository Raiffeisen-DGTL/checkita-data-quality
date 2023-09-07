package ru.raiffeisen.checkita.utils.io.dbmanager

import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.utils.{DQSettings, Logging}

import java.sql.Timestamp
import java.time.ZoneId
import scala.util.Try

final class HiveDBManager (settings: DQSettings,
                     spark: SparkSession) extends SparkDBManager with Logging {

  private val hiveSchema: Option[String] = settings.resStorage.flatMap(_.schema)

  def saveResultsToDB(metrics: Seq[AnyRef],
                      tb: String): Unit = hiveSchema match {
    case Some(schema) =>
      val table = s"$schema.$tb"

      log.info(s"Saving to Hive $table...")
      try {
        val resultSchema = buildSchema(metrics.head)

        // Hive tables are partitioned by job_id and, therefore, it must be at last position in dataframe:
        val shuffledSchema = StructType(
          resultSchema.filter(_.name != "job_id") ++ resultSchema.filter(_.name == "job_id")
        )
        val uniqueCols = uniqueFields(tb)

        val curJobId = metrics.head.getClass.getDeclaredFields
          .filter(f => f.getName == "jobId").map{ f =>
          f.setAccessible(true)
          f.get(metrics.head).asInstanceOf[String]
        }.head

        val currentData = spark.read.table(table)
          .filter(s"job_id='$curJobId'")

        val resultsData = updateData(spark, currentData, metrics, resultSchema, uniqueCols)

        resultsData.select(shuffledSchema.map(c => col(c.name)) : _*)
          .repartition(1).write.mode(SaveMode.Overwrite).insertInto(table)

        log.info("Success!")
      } catch {
        case _: NoSuchElementException =>
          log.warn("Nothing to save!")
        case e: AnalysisException =>
          log.error("Failed to load with spark error:")
          log.error(e.toString)
        case e: Exception =>
          log.error("Failed with error:")
          log.error(e.toString)
      }
    case None => log.warn("History storage is not connected. Avoiding saving the results...")
  }

  def loadResults[T](metricSet: List[String],
                     jobId: String,
                     rule: String,
                     tw: Int,
                     startDate: Timestamp,
                     tb: String)(f: Row => Seq[T]): Seq[T] = hiveSchema match {
    case Some(schema) =>
      val table = s"$schema.$tb"
      log.info(s"Loading results from Hive '$table'...")

      val currentData = Try(spark.read.table(table)).toOption

      val resultSet: Seq[T] = currentData.map{ df =>
        val preDf = df.filter(s"job_id='$jobId'")
          .filter(col("metric_id").isin(metricSet:_*))
          .filter(col("reference_date") <= startDate)

        // move job_id column to the first position in order to match result converters
        val targetSchema = preDf.schema.filter(_.name == "job_id") ++ preDf.schema.filter(_.name != "job_id")
        val preDfFmt = preDf.select(targetSchema.map(c => col(c.name)) : _*)

        val results = rule match {
          case "record" =>
            preDfFmt.orderBy(col("reference_date").desc).limit(tw * metricSet.length).collect().toSeq
          case "date" =>
            val lastDate =Timestamp.from(startDate.toInstant.atZone(ZoneId.of("UTC")).minusDays(tw).toInstant)

            log.info(s"Hive DB Manager -> Load Results -> from_date = ${lastDate.toString}")
            log.info(s"Hive DB Manager -> Load Results -> until_date = ${startDate.toString}")

            preDfFmt.filter(col("reference_date") >= lastDate).collect().toSeq
          case x => throw IllegalParameterException(x)
        }

        log.info("Hive DB Manager -> Load Results -> following results are loaded:")
        results.foreach(r => log.info(r.toString))
        results.flatMap(f)
      }.getOrElse(Seq.empty[T])

      if (currentData.isEmpty)
        log.warn("History storage table is absent or is empty. Providing empty historical results...")
      else
        log.info(s"Results found: ${resultSet.length}")
      resultSet
    case None =>
      log.warn("History database is not connected. Providing empty historical results...")
      Seq.empty[T]
  }
}
