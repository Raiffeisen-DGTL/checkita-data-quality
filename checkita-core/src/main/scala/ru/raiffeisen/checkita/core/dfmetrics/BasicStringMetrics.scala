package ru.raiffeisen.checkita.core.dfmetrics
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import ru.raiffeisen.checkita.core.metrics.MetricName


object BasicStringMetrics {

  case class DFRegexMatchCalculator(metricId: String,
                                    columns: Seq[String],
                                    regex: String) extends DFMetricCalculator {

    override val metricName: MetricName = MetricName.RegexMatch
    override val errorMessage: String = "Some of the values failed to match regex pattern."

    override protected val resultExpr: Column = columns.map { c =>
      when(col(c).cast(StringType).rlike(regex), lit(1)).otherwise(lit(0))
    }.foldLeft(lit(0))(_ + _)
    override protected val errorConditionExpr: Column = resultExpr < lit(columns.size)
    override protected val resultAggregateFunction: Column => Column = sum
  }

  case class DFNullValuesMetricCalculator(metricId: String,
                                          columns: Seq[String]) extends DFMetricCalculator {

    override val metricName: MetricName = MetricName.NullValues
    override val errorMessage: String =
      s"There are nulls values found within specified columns of processed row."
    override protected val resultExpr: Column = columns.map { c =>
      when(col(c).isNull, lit(1)).otherwise(lit(0))
    }.foldLeft(lit(0))(_ + _)
    override protected val errorConditionExpr: Column = resultExpr > lit(0)
    override protected val resultAggregateFunction: Column => Column = sum
  }

  case class DFCompletenessMetricCalculator(metricId: String,
                                            columns: Seq[String],
                                            includeEmptyStrings: Boolean) extends DFMetricCalculator {

    override val metricName: MetricName = MetricName.Completeness
    override val errorMessage: String =
      if (includeEmptyStrings)
        "There are null or empty string values found within specified columns of processed row."
      else s"There are nulls values found within specified columns of processed row."

    override protected val resultExpr: Column = columns.map{ c =>
      when(col(c).isNull, lit(0))
        .when(lit(includeEmptyStrings) && col(c).cast(StringType) === lit(""), lit(0))
        .otherwise(lit(1))
    }.foldLeft(lit(0))(_ + _)

    override protected val errorConditionExpr: Column = resultExpr < lit(columns.size)
    override protected val resultAggregateFunction: Column => Column =
      rowRes => sum(rowRes) / count(lit(1)) / lit(columns.size)
  }

  case class DFMaxStringValueMetricCalculator(metricId: String,
                                              columns: Seq[String]) extends DFMetricCalculator {

    override val metricName: MetricName = MetricName.MaxString
    override val errorMessage: String = "Maximum string length cannot be calculated for provided row values."

    override protected val resultExpr: Column =
      if (columns.size == 1) length(col(columns.head).cast(StringType))
      else greatest(columns.map(c => length(col(c).cast(StringType))): _*)

    override protected val errorConditionExpr: Column = resultExpr.isNull
    override protected val resultAggregateFunction: Column => Column = max
  }

  case class DFStringInDomainValuesMetricCalculator(metricId: String,
                                                    columns: Seq[String],
                                                    domain: Set[String]) extends DFMetricCalculator {

    override val metricName: MetricName = MetricName.StringInDomain
    override val errorMessage: String =
      s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}"

    override protected val resultExpr: Column = columns.map { c =>
      when(col(c).cast(StringType).isInCollection(domain), lit(1)).otherwise(lit(0))
    }.foldLeft(lit(0))(_ + _)

    override protected val errorConditionExpr: Column = resultExpr < lit(columns.size)
    override protected val resultAggregateFunction: Column => Column = sum
  }
}
