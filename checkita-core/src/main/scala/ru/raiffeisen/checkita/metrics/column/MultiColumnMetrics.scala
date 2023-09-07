package ru.raiffeisen.checkita.metrics.column

import org.apache.commons.text.similarity.LevenshteinDistance
import org.joda.time.Days
import ru.raiffeisen.checkita.metrics.{CalculatorStatus, MetricCalculator, StatusableCalculator}
import ru.raiffeisen.checkita.metrics.CalculatorStatus.CalculatorStatus
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.utils.{Logging, getParametrizedMetricTail, tryToDate, tryToDouble, tryToString}

import scala.util.Try


object MultiColumnMetrics extends Logging {

  /**
   * Calculates covariance between values of two columns
   * @param lMean Mean of the first column
   * @param rMean Mean of the second column
   * @param coMoment current co-moment
   * @param n number of records
   * @param status current calculator status
   * @param failCount current fail counter
   *
   * (Idea here is that in order to correctly calculate covariance of the two columns,
   * all cells' values must be convertable to Double. Otherwise the metric result won't
   * be reliable, and, NaN value will be returned instead)
   */
  case class CovarianceMetricCalculator(lMean: Double,
                                        rMean: Double,
                                        coMoment: Double,
                                        n: Long,
                                        protected val status: CalculatorStatus = CalculatorStatus.OK,
                                        protected val failCount: Int = 0)
    extends StatusableCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0, 0, 0, 0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 2, "covariance metric works with two columns only!")
      val lOpt = tryToDouble(values.head)
      val rOpt = tryToDouble(values.tail.head)

      (lOpt, rOpt) match {
        case (Some(l), Some(r)) =>
          val newN = n + 1
          val lm = lMean + (l - lMean) / newN
          val rm = rMean + (r - rMean) / newN
          val cm = coMoment + (l - lMean) * (r - rm)
          CovarianceMetricCalculator(lm, rm, cm, newN, CalculatorStatus.OK, this.failCount)
        case (None, _) | (_, None) => this.copy(
          status=CalculatorStatus.FAILED,
          failCount=failCount + 1
        )
      }
    }

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val that: CovarianceMetricCalculator = m2.asInstanceOf[CovarianceMetricCalculator]
      CovarianceMetricCalculator(
        (this.lMean * this.n + that.lMean * that.n) / (this.n + that.n),
        (this.rMean * this.n + that.rMean * that.n) / (this.n + that.n),
        this.coMoment + that.coMoment +
          (this.lMean - that.lMean) * (this.rMean - that.rMean) * ((this.n * that.n) / (this.n + that.n)),
        this.n + that.n,
        this.status,
        this.failCount + that.getFailCounter
      )
    }

    override def result(): Map[String, (Double, Option[String])] = if (failCount == 0 && n > 0) {
      Map(
        "CO_MOMENT" -> (coMoment, None),
        "COVARIANCE" -> (coMoment / n, None),
        "COVARIANCE_BESSEL" -> (coMoment / (n - 1), None)
      )
    } else {
      Map(
        "CO_MOMENT" -> (Double.NaN, None),
        "COVARIANCE" -> (Double.NaN, None),
        "COVARIANCE_BESSEL" -> (Double.NaN, None)
      )
    }
  }

  /**
   * Calculates amount of equal rows
   * @param cnt current counter
   * @param status current calculator status (fails if lValue != rValue)
   * @param failCount current fail counter
   */
  case class EqualStringColumnsMetricCalculator(cnt: Int,
                                                protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                protected val failCount: Int = 0)
    extends StatusableCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val valuesStr = values.flatMap(tryToString)

      if (values.length == valuesStr.length) {
        if (valuesStr.distinct.length == 1) this.copy(cnt=cnt+1, status=CalculatorStatus.OK)
        else EqualStringColumnsMetricCalculator(
          cnt, CalculatorStatus.FAILED, failCount + 1
        )
      }
      else EqualStringColumnsMetricCalculator(
        cnt, CalculatorStatus.FAILED, failCount + 1
      ) // some of the values in sequence are not castable to string
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("COLUMN_EQ" -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2Casted = m2.asInstanceOf[EqualStringColumnsMetricCalculator]
      EqualStringColumnsMetricCalculator(
        this.cnt + m2Casted.cnt,
        this.status,
        this.failCount + m2Casted.getFailCounter
      )
    }
  }

  /**
   * calculate the number of the rows for which the day difference btw
   * the two columns given as input is les than the threshold: day-treshold"
   *   "FORMATTED_DATE"
   */
  case class DayDistanceMetric(cnt: Double,
                               paramMap: ParamMap,
                               protected val status: CalculatorStatus = CalculatorStatus.OK,
                               protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val dateFormat = Try(paramMap("dateFormat").toString).toOption.getOrElse("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    private val dayThreshold: Int = paramMap("threshold").toString.toInt

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 2, "dayDistance metric works with two columns only!")
      val firstDate = tryToDate(values.head, dateFormat)
      val secondDate = tryToDate(values.tail.head, dateFormat)
      val result = (firstDate, secondDate) match {
        case (Some(dateA), Some(dateB)) =>
          Math.abs(Days.daysBetween(dateA, dateB).getDays) < dayThreshold
        case _ => false
      }
      if (result) this.copy(cnt=cnt + 1, status=CalculatorStatus.OK)
      else DayDistanceMetric(
        cnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + 1
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("DAY_DISTANCE" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2Casted = m2.asInstanceOf[DayDistanceMetric]
      DayDistanceMetric(
        this.cnt + m2Casted.cnt,
        paramMap,
        status,
        this.getFailCounter + m2Casted.getFailCounter
      )
    }
  }

  /**
   * Calculates amount of rows where Levenshtein distance between 2 columns
   * is lesser than threshold.
   * @param cnt current success counter
   * @param paramMap paramMap. May contains:
   *   required "threshold" - distance threshold (should be within [0, 1] range for normalized results)
   *   optional "normalize" - boolean flag to define whether distance should be normalized
   *   over maximum length of two input strings (default is false).
   * @param status current calculator status (fails if distance > distanceThreshold)
   * @param failCount current fail counter
   */
  case class LevenshteinDistanceMetric(cnt: Double,
                                       paramMap: ParamMap,
                                       protected val status: CalculatorStatus = CalculatorStatus.OK,
                                       protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val distanceThreshold: Double = paramMap("threshold").toString.toDouble
    private val isNormalized: Boolean = Try(paramMap("normalize").toString.toBoolean).getOrElse(false)

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 2, "levenshteinDistance metric works with two columns only!")
      if (isNormalized) assert(
        0 <= distanceThreshold && distanceThreshold <= 1,
        "levenshteinDistance metric threshold should be within [0, 1] range when normalized set to true."
      )
      val firstVal = tryToString(values.head)
      val secondVal = tryToString(values.tail.head)
      val result: Boolean = (firstVal, secondVal) match {
        case (Some(x), Some(y)) =>
          val cleanX = x.trim().toUpperCase
          val cleanY = y.trim().toUpperCase
          val distance = LevenshteinDistance.getDefaultInstance.apply(cleanX, cleanY)
          if (isNormalized) {
            val normalization = math.max(cleanX.length, cleanY.length).toDouble
            (distance / normalization) <= distanceThreshold
          }
          else distance <= distanceThreshold
        case _ => false
      }
      if (result) this.copy(cnt=cnt + 1, status=CalculatorStatus.OK)
      else LevenshteinDistanceMetric(
        cnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + 1
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("LEVENSHTEIN_DISTANCE" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[LevenshteinDistanceMetric]
      LevenshteinDistanceMetric(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }

}
