package ru.raiffeisen.checkita.core.metrics.regular

import org.apache.commons.text.similarity.LevenshteinDistance
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Helpers.{tryToDate, tryToDouble, tryToString}
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName}

import java.time.temporal.ChronoUnit.DAYS

object MultiColumnMetrics {

  /**
   * Calculates covariance between values of two columns
   * @param lMean Mean of the first column
   * @param rMean Mean of the second column
   * @param coMoment current co-moment
   * @param n number of records
   * (Idea here is that in order to correctly calculate covariance of the two columns,
   * all cells' values must be convertable to Double. Otherwise the metric result won't
   * be reliable, and, NaN value will be returned instead)
   * @return result map with keys:
   *         - "CO_MOMENT"
   *         - "COVARIANCE"
   *         - "COVARIANCE_BESSEL"
   */
  case class CovarianceMetricCalculator(lMean: Double,
                                        rMean: Double,
                                        coMoment: Double,
                                        n: Long,
                                        protected val failCount: Long = 0,
                                        protected val status: CalculatorStatus = CalculatorStatus.Success,
                                        protected val failMsg: String = "OK") extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, 0, 0, 0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 2, "covariance metric works with two columns only!")
      val lOpt = tryToDouble(values.head)
      val rOpt = tryToDouble(values.tail.head)

      (lOpt, rOpt) match {
        case (Some(l), Some(r)) =>
          val newN = n + 1
          val lm = lMean + (l - lMean) / newN
          val rm = rMean + (r - rMean) / newN
          val cm = coMoment + (l - lMean) * (r - rm)
          CovarianceMetricCalculator(lm, rm, cm, newN, failCount)
        case (None, _) | (_, None) => copyWithError(
          CalculatorStatus.Failure,
          "Some of the provided values cannot be casted to number"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    override def result(): Map[String, (Double, Option[String])] = if (failCount == 0 && n > 0) {
      Map(
        MetricName.CoMoment.entryName -> (coMoment, None),
        MetricName.Covariance.entryName -> (coMoment / n, None),
        MetricName.CovarianceBessel.entryName -> (coMoment / (n - 1), None)
      )
    } else {
      val msg = Some("Metric calculation failed due to some of the processed values cannot be casted to number.")
      Map(
        MetricName.CoMoment.entryName -> (Double.NaN, msg),
        MetricName.Covariance.entryName -> (Double.NaN, msg),
        MetricName.CovarianceBessel.entryName -> (Double.NaN, msg)
      )
    }
    
    override def merge(m2: MetricCalculator): MetricCalculator = {
      val that: CovarianceMetricCalculator = m2.asInstanceOf[CovarianceMetricCalculator]
      CovarianceMetricCalculator(
        (this.lMean * this.n + that.lMean * that.n) / (this.n + that.n),
        (this.rMean * this.n + that.rMean * that.n) / (this.n + that.n),
        this.coMoment + that.coMoment +
          (this.lMean - that.lMean) * (this.rMean - that.rMean) * ((this.n * that.n) / (this.n + that.n)),
        this.n + that.n,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount rows where elements in the given columns are equal
   * @param cnt current counter
   * @return result map with keys: "COLUMN_EQ"
   */
  case class EqualStringColumnsMetricCalculator(cnt: Int,
                                                protected val failCount: Long = 0,
                                                protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                protected val failMsg: String = "OK") extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val valuesStr = values.flatMap(tryToString)
      
      if (values.length == valuesStr.length) {
        if (valuesStr.distinct.length == 1) EqualStringColumnsMetricCalculator(cnt + 1, failCount)
        else copyWithError(CalculatorStatus.Failure, "Provided values are not equal")
      }
      else copyWithError(CalculatorStatus.Failure, "Some of the provided values cannot be casted to string")
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
      
    def result(): Map[String, (Double, Option[String])] = 
      Map(MetricName.ColumnEq.entryName -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[EqualStringColumnsMetricCalculator]
      EqualStringColumnsMetricCalculator(
        this.cnt + that.cnt,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates the number of the rows for which the day difference between
   * two columns given as input is less than the threshold (number of days)
   * @param cnt Current coutner
   * @param dateFormat Date format for values in columns
   * @param threshold Maximum allowed day distance between dates in columns
   * @return result map with keys: "DAY_DISTANCE"
   */
  case class DayDistanceMetricCalculator(cnt: Double,
                                         dateFormat: String,
                                         threshold: Int,
                                         protected val failCount: Long = 0,
                                         protected val status: CalculatorStatus = CalculatorStatus.Success,
                                         protected val failMsg: String = "OK") extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(dateFormat: String, threshold: Int) = this(0, dateFormat, threshold)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 2, "dayDistance metric works with two columns only!")
      val dates = for {
        firstDate <- tryToDate(values.head, dateFormat)
        secondDate <- tryToDate(values.tail.head, dateFormat)
      } yield (firstDate, secondDate)
      
      dates.map {
        case (firstDate, secondDate) =>
          if (Math.abs(DAYS.between(firstDate, secondDate)) < threshold)
            DayDistanceMetricCalculator(cnt + 1, dateFormat, threshold, failCount)
          else copyWithError(
            CalculatorStatus.Failure,
            s"Distance between two dates is greater than or equal to given threshold of '$threshold'"
          )
      }.getOrElse(copyWithError(
        CalculatorStatus.Failure,
        s"Some of the provided values cannot be casted to date with given format of '$dateFormat'."
      ))
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    override def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.DayDistance.entryName -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[DayDistanceMetricCalculator]
      DayDistanceMetricCalculator(
        this.cnt + that.cnt,
        this.dateFormat,
        this.threshold,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of rows where Levenshtein distance between 2 columns is less than threshold.
   * @param cnt current success counter
   * @param threshold Threshold (should be within [0, 1] range for normalized results)
   * @param normalize Flag to define whether distance should be normalized over maximum length of two input strings
   * @return result map with keys: "LEVENSHTEIN_DISTANCE"                
   */
  case class LevenshteinDistanceMetricCalculator(cnt: Double,
                                                 threshold: Double,
                                                 normalize: Boolean,
                                                 protected val failCount: Long = 0,
                                                 protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                 protected val failMsg: String = "OK") extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(threshold: Double, normalize: Boolean) = this(0, threshold, normalize)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 2, "levenshteinDistance metric works with two columns only!")
      if (normalize) assert(
        0 <= threshold && threshold <= 1,
        "levenshteinDistance metric threshold should be within [0, 1] range when normalized set to true."
      )
      val stringValues = for {
        firstVal <- tryToString(values.head)
        secondVal <- tryToString(values.tail.head)
      } yield (firstVal, secondVal)

      stringValues.map {
        case (x, y) =>
          val levDist = getLevenshteinDistance(x, y)
          if (levDist < threshold) LevenshteinDistanceMetricCalculator(cnt + 1, threshold, normalize, failCount)
          else copyWithError(
            CalculatorStatus.Failure, 
            s"Levenshtein distance for given values is grater than or equal to given threshold of '$threshold'"
          )
      }.getOrElse(copyWithError(
        CalculatorStatus.Failure,
        "Some of the provided values cannot be casted to string"
      ))
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
      
    override def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.LevenshteinDistance.entryName -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[LevenshteinDistanceMetricCalculator]
      LevenshteinDistanceMetricCalculator(
        this.cnt + that.cnt,
        this.threshold,
        this.normalize,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
    
    private def getLevenshteinDistance(x: String, y: String): Double = {
      val cleanX = x.trim().toUpperCase
      val cleanY = y.trim().toUpperCase
      val distance = LevenshteinDistance.getDefaultInstance.apply(cleanX, cleanY)
      
      if (normalize) distance / Math.max(cleanX.length, cleanY.length).toDouble else distance.toDouble
    }
  }

}
