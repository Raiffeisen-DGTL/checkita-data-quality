package ru.raiffeisen.checkita.metrics.composed

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.metrics.{ColumnMetricResult, ComposedMetric, ComposedMetricCalculator, FileMetricResult}
import ru.raiffeisen.checkita.utils.{DQCommandLineOptions, DQSettings}

class ComposedMetricSpec extends AnyWordSpec with Matchers {
  private val applicationConf: String = "checkita-core/src/test/resources/appConfig/test.conf"
  private val configFilePath: String = "dummy/config/file/path.conf"
  private val args: Array[String] = Array("-a=" + applicationConf,"-c=" + configFilePath)
  private implicit val settings: DQSettings = DQCommandLineOptions.parser().parse(args, DQCommandLineOptions("","")) match {
    case Some(commandLineOptions) => DQSettings(commandLineOptions)
  }
  private val metricResults = Seq(
    ColumnMetricResult("job1", "hive_table1_nulls", "2000-01-01", "nullValues", "hive_table1", Seq("id", "name"), "", 5.0, ""),
    FileMetricResult("job1", "hive_table1_row_cnt", "2000-01-01", "file", "hive_table1", 10.0, "")
  )

  "ComposedMetricCalculator" must {
    "in addition and subtraction test with two metrics return 12" in {
      val composedMetric =
        ComposedMetric(
          "test",
          "COMPOSED",
          "addition and subtraction",
          "$hive_table1_nulls + $hive_table1_row_cnt - 3",
          Map.empty
        )
      val result = 12.0
      new ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "in multiplication and division test with two metrics return 50" in {
      val composedMetric =
        ComposedMetric(
          "test",
          "COMPOSED",
          "multiplication and division",
          "100 * $hive_table1_nulls / $hive_table1_row_cnt",
          Map.empty
        )
      val result = 50.0
      new ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "in multiplication, division and power test with one metric return 1000" in {
      val composedMetric =
        ComposedMetric(
          "test",
          "COMPOSED",
          "multiplication, division and power",
          "5 * $hive_table1_row_cnt ^ 2 * 2",
          Map.empty
        )
      val result = 1000.0
      new ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "in all operations test with two metrics must return 200" in {
      val composedMetric =
        ComposedMetric(
          "test",
          "COMPOSED",
          "all",
          "($hive_table1_row_cnt ^ 2 / 10 + ($hive_table1_nulls * 2 - 9) ^ 10 + 4) ^ 2 - $hive_table1_row_cnt / 5 - 23",
          Map.empty
        )
      val result = 200.0
      new ComposedMetricCalculator(metricResults).run(composedMetric).result shouldEqual result
    }

    "in wrong metric name test throw NoSuchElementException exception" in {
      val composedMetric =
        ComposedMetric(
          "test",
          "COMPOSED",
          "error",
          "100 * $hive_table1_values / $hive_table1_row_cnt",
          Map.empty
        )
      an [NoSuchElementException] should be thrownBy
        new ComposedMetricCalculator(metricResults).run(composedMetric).result
    }
  }
}
