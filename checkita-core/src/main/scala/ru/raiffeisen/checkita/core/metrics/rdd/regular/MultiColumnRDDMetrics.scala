package ru.raiffeisen.checkita.core.metrics.rdd.regular

import org.apache.commons.text.similarity.LevenshteinDistance
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.rdd.Casting.{tryToDate, tryToDouble, tryToString}
import ru.raiffeisen.checkita.core.metrics.rdd.{RDDMetricCalculator, ReversibleRDDCalculator}
import ru.raiffeisen.checkita.core.metrics.MetricName

import java.time.temporal.ChronoUnit.DAYS

object MultiColumnRDDMetrics {

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
  case class CovarianceRDDMetricCalculator(lMean: Double,
                                           rMean: Double,
                                           coMoment: Double,
                                           n: Long,
                                           protected val failCount: Long = 0,
                                           protected val status: CalculatorStatus = CalculatorStatus.Success,
                                           protected val failMsg: String = "OK") extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, 0, 0, 0)
    
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      assert(values.length == 2, "covariance metric works with two columns only!")
      val lOpt = tryToDouble(values.head)
      val rOpt = tryToDouble(values.tail.head)

      (lOpt, rOpt) match {
        case (Some(l), Some(r)) =>
          val newN = n + 1
          val lm = lMean + (l - lMean) / newN
          val rm = rMean + (r - rMean) / newN
          val cm = coMoment + (l - lMean) * (r - rm)
          CovarianceRDDMetricCalculator(lm, rm, cm, newN, failCount)
        case (None, _) | (_, None) => copyWithError(
          CalculatorStatus.Failure,
          "Some of the provided values cannot be cast to number"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    override def result(): Map[String, (Double, Option[String])] = {
      val coMomentAdj = if (n > 0) coMoment else Double.NaN  // return NaN from empty calculator state
      val covariance = if (n > 0) coMoment / n else Double.NaN
      val covarianceBessel = if (n > 1) coMoment / (n - 1) else Double.NaN
      Map(
        MetricName.CoMoment.entryName -> (coMomentAdj, None),
        MetricName.Covariance.entryName -> (covariance, None),
        MetricName.CovarianceBessel.entryName -> (covarianceBessel, None)
      )
    }
    
    override def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that: CovarianceRDDMetricCalculator = m2.asInstanceOf[CovarianceRDDMetricCalculator]
      if (this.n == 0) that else CovarianceRDDMetricCalculator(
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
  case class ColumnEqRDDMetricCalculator(cnt: Int,
                                         protected val reversed: Boolean,
                                         protected val failCount: Long = 0,
                                         protected val status: CalculatorStatus = CalculatorStatus.Success,
                                         protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(reversed: Boolean) = this(0, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that rows where string values in requested columns are not equal
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val valuesStr = values.flatMap(tryToString)
      
      if (values.length == valuesStr.length) {
        if (valuesStr.distinct.length == 1) ColumnEqRDDMetricCalculator(cnt + 1, reversed, failCount)
        else copyWithError(CalculatorStatus.Failure, "Provided values are not equal.")
      }
      else copyWithError(CalculatorStatus.Failure, "Some of the provided values cannot be cast to string.")
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that rows where string values in requested columns ARE equal
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val valuesStr = values.flatMap(tryToString)

      if (values.length == valuesStr.length) {
        if (valuesStr.distinct.length == 1) ColumnEqRDDMetricCalculator(
          cnt + 1, reversed, failCount + 1, CalculatorStatus.Failure, "Provided values ARE equal.")
        else ColumnEqRDDMetricCalculator(cnt, reversed, failCount)
      }
      else copyWithError(CalculatorStatus.Failure, "Some of the provided values cannot be cast to string.")
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
      
    def result(): Map[String, (Double, Option[String])] = 
      Map(MetricName.ColumnEq.entryName -> (cnt.toDouble, None))

    override def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[ColumnEqRDDMetricCalculator]
      ColumnEqRDDMetricCalculator(
        this.cnt + that.cnt,
        this.reversed,
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
  case class DayDistanceRDDMetricCalculator(cnt: Double,
                                            dateFormat: String,
                                            threshold: Int,
                                            protected val reversed: Boolean,
                                            protected val failCount: Long = 0,
                                            protected val status: CalculatorStatus = CalculatorStatus.Success,
                                            protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(dateFormat: String, threshold: Int, reversed: Boolean) = this(0, dateFormat, threshold, reversed)

    /**
     * Common logic for incrementing metric calculator (applies to both direct and reversed error collection logic).
     *
     * @param values               values to process
     * @param incrementedOutput    Instance of metric calculator with incremented counter.
     * @param notIncrementedOutput Instance of metric calculator for case when counter is not incremented.
     * @return Updated calculator or throws an exception
     */
    private def incrementer(values: Seq[Any],
                            incrementedOutput: RDDMetricCalculator,
                            notIncrementedOutput: RDDMetricCalculator): RDDMetricCalculator = {
      assert(values.length == 2, "dayDistance metric works with two columns only!")
      val dates = for {
        firstDate <- tryToDate(values.head, dateFormat)
        secondDate <- tryToDate(values.tail.head, dateFormat)
      } yield (firstDate, secondDate)

      dates.map {
        case (firstDate, secondDate) =>
          if (Math.abs(DAYS.between(firstDate, secondDate)) < threshold) incrementedOutput
          else notIncrementedOutput
      }.getOrElse(copyWithError(
        CalculatorStatus.Failure,
        s"Some of the provided values cannot be cast to date with given format of '$dateFormat'."
      ))
    }

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that rows where date distance between two dates is greater than or
     * equal to provided threshold are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = incrementer(
      values,
      DayDistanceRDDMetricCalculator(cnt + 1, dateFormat, threshold, reversed, failCount),
      copyWithError(
        CalculatorStatus.Failure,
        s"Distance between two dates is greater than or equal to given threshold of '$threshold'"
      )
    )

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that rows where date distance between two dates is less than
     * provided threshold are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = incrementer(
      values,
      DayDistanceRDDMetricCalculator(
        cnt + 1,
        dateFormat,
        threshold,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Distance between two dates lower than given threshold of '$threshold'"
      ),
      DayDistanceRDDMetricCalculator(cnt, dateFormat, threshold, reversed, failCount)
    )

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    override def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.DayDistance.entryName -> (cnt, None))

    override def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[DayDistanceRDDMetricCalculator]
      DayDistanceRDDMetricCalculator(
        this.cnt + that.cnt,
        this.dateFormat,
        this.threshold,
        this.reversed,
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
  case class LevenshteinDistanceRDDMetricCalculator(cnt: Double,
                                                    threshold: Double,
                                                    normalize: Boolean,
                                                    protected val reversed: Boolean,
                                                    protected val failCount: Long = 0,
                                                    protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                    protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(threshold: Double, normalize: Boolean, reversed: Boolean) = this(0, threshold, normalize, reversed)

    /**
     * Common logic for incrementing metric calculator (applies to both direct and reversed error collection logic).
     *
     * @param values               values to process
     * @param incrementedOutput    Instance of metric calculator with incremented counter.
     * @param notIncrementedOutput Instance of metric calculator for case when counter is not incremented.
     * @return Updated calculator or throws an exception
     */
    private def incrementer(values: Seq[Any],
                            incrementedOutput: RDDMetricCalculator,
                            notIncrementedOutput: RDDMetricCalculator): RDDMetricCalculator = {
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
          if (levDist < threshold) incrementedOutput
          else notIncrementedOutput
      }.getOrElse(copyWithError(
        CalculatorStatus.Failure,
        "Some of the provided values cannot be cast to string"
      ))
    }

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that rows where levenshtein distance between two string values
     * is greater than or equal to the provided threshold are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = incrementer(
      values,
      LevenshteinDistanceRDDMetricCalculator(cnt + 1, threshold, normalize, reversed, failCount),
      copyWithError(
        CalculatorStatus.Failure,
        s"Levenshtein distance for given values is grater than or equal to given threshold of '$threshold'"
      )
    )

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that rows where levenshtein distance between two string values
     * is lower than the provided threshold are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = incrementer(
      values,
      LevenshteinDistanceRDDMetricCalculator(
        cnt + 1,
        threshold,
        normalize,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Levenshtein distance for given values is lower than given threshold of '$threshold'"
      ),
      LevenshteinDistanceRDDMetricCalculator(cnt, threshold, normalize, reversed, failCount)
    )

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
      
    override def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.LevenshteinDistance.entryName -> (cnt, None))

    override def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[LevenshteinDistanceRDDMetricCalculator]
      LevenshteinDistanceRDDMetricCalculator(
        this.cnt + that.cnt,
        this.threshold,
        this.normalize,
        this.reversed,
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
