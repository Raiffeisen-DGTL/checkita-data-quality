package ru.raiffeisen.checkita.core.metrics.rdd.regular

import org.isarnproject.sketches.java.TDigest
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.metrics.rdd.Casting.{tryToDouble, tryToLong}
import ru.raiffeisen.checkita.core.metrics.rdd.{RDDMetricCalculator, ReversibleRDDCalculator}
import ru.raiffeisen.checkita.core.metrics.MetricName

import scala.util.Try

/**
 * Basic metrics that can be applied to numerical elements
 */
object BasicNumericRDDMetrics {

  /**
   * Calculates percentiles, quantiles for provided elements with use of TDigest library
   * https://github.com/isarn/isarn-sketches
   *
   * Works for single column only!
   *
   * @param tdigest Initial TDigest object
   * @param accuracyError Required level of calculation accuracy
   * @param targetSideNumber Required parameter. For quantiles should be in [0,1]
   *
   * @return result map with keys:
   *         - "GET_QUANTILE"
   *         - "GET_PERCENTILE"
   *         - "FIRST_QUANTILE"
   *         - "THIRD_QUANTILE"
   *         - "MEDIAN_VALUE"
   */
  case class TDigestRDDMetricCalculator(tdigest: TDigest,
                                        accuracyError: Double,
                                        targetSideNumber: Double,
                                        protected val failCount: Long = 0,
                                        protected val status: CalculatorStatus = CalculatorStatus.Success,
                                        protected val failMsg: String = "OK")
    extends RDDMetricCalculator {
    
    // axillary constructor to initiate with empty TDigest object
    def this(accuracyError: Double, targetSideNumber: Double) =
      this(TDigest.empty(accuracyError), accuracyError, targetSideNumber)

    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      assert(values.length == 1, "TDigest metrics work for single column only!")
      tryToDouble(values.head) match {
        case Some(v) => 
          tdigest.update(v)
          TDigestRDDMetricCalculator(tdigest, accuracyError, targetSideNumber, failCount)
        case None    => copyWithError(CalculatorStatus.Failure,
          "Provided value cannot be cast to number."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] = {

      val staticResults: Map[String, (Double, Option[String])] = Map(
        MetricName.MedianValue.entryName -> (tdigest.cdfInverse(0.5), None),
        MetricName.FirstQuantile.entryName -> (tdigest.cdfInverse(0.25), None),
        MetricName.ThirdQuantile.entryName -> (tdigest.cdfInverse(0.75), None)
      )
      
      val parametrizedResults: Map[String, (Double, Option[String])] = targetSideNumber match {
        case x if x >= 0 && x <= 1 => Map(
          MetricName.GetQuantile.entryName -> (tdigest.cdfInverse(x), None),
          MetricName.GetPercentile.entryName -> (tdigest.cdf(x), None)
        )
        case x => Map(MetricName.GetPercentile.entryName -> (tdigest.cdf(x), None))
      }
      staticResults ++ parametrizedResults
    }

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[TDigestRDDMetricCalculator]
      TDigestRDDMetricCalculator(
        TDigest.merge(this.tdigest, that.tdigest),
        this.accuracyError,
        this.targetSideNumber,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg)
    }
  }

  /**
   * Calculates minimal value for provided elements
   * @param min Current minimal value
   *
   * @return result map with keys: "MIN_NUMBER"
   */
  case class MinNumberRDDMetricCalculator(min: Double,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Double.MaxValue)
    
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      // .min will throw "UnsupportedOperationException" when applied to empty sequence.
      val rowMinValue = Try(values.flatMap(tryToDouble).min).toOption
      rowMinValue match {
        case Some(v) => MinNumberRDDMetricCalculator(Math.min(v, min), failCount)
        case None    => copyWithError(
          CalculatorStatus.Failure,
          "Failed to calculate minimum number out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MinNumber.entryName -> (min, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[MinNumberRDDMetricCalculator]
      MinNumberRDDMetricCalculator(
        Math.min(this.min, that.min),
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Calculates maximal value for provided elements
   * @param max Current maximal value
   * @return result map with keys: "MAX_NUMBER"
   */
  case class MaxNumberRDDMetricCalculator(max: Double,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Double.MinValue)
    
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      // .max will throw "UnsupportedOperationException" when applied to empty sequence.
      val rowMaxValue = Try(values.flatMap(tryToDouble).max).toOption
      rowMaxValue match {
        case Some(v) => MaxNumberRDDMetricCalculator(Math.max(v, max), failCount)
        case None    => copyWithError(
          CalculatorStatus.Failure,
          "Failed to calculate maximum number out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MaxNumber.entryName -> (max, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[MaxNumberRDDMetricCalculator]
      MaxNumberRDDMetricCalculator(
        Math.max(this.max, that.max),
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates sum of provided elements
   * @param sum Current sum
   *
   * @return result map with keys: "SUM_NUMBER"
   */
  case class SumNumberRDDMetricCalculator(sum: Double,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0)
    
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val doubleValues = values.flatMap(tryToDouble)
      if (doubleValues.size == values.size) SumNumberRDDMetricCalculator(sum + doubleValues.sum, failCount)
      else SumNumberRDDMetricCalculator(
        sum + doubleValues.sum,
        failCount + 1,
        CalculatorStatus.Failure,
        "Some of the provided values cannot be cast to number."
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] = 
      Map(MetricName.SumNumber.entryName -> (sum, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[SumNumberRDDMetricCalculator]
      SumNumberRDDMetricCalculator(
        this.sum + that.sum, 
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Calculates standard deviation and mean value for provided elements
   *
   * Works for single column only!
   *
   * @param sum Current sum
   * @param sqSum Current squared sum
   * @param cnt Current element count
   *
   * @return result map with keys:
   *   "STD_NUMBER"
   *   "AVG_NUMBER"
   */
  case class StdAvgNumberRDDMetricCalculator(sum: Double,
                                             sqSum: Double,
                                             cnt: Long,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, 0, 0)
    
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      assert(values.length == 1, "avgNumber and stdNumber metrics work for single column only!")
      tryToDouble(values.head) match {
        case Some(v) => StdAvgNumberRDDMetricCalculator(sum + v, sqSum + (v * v), cnt + 1, failCount)
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Provided value cannot be cast to number."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] = {
      val mean = sum / cnt.toDouble
      val variance = sqSum / cnt.toDouble - mean * mean
      Map(
        MetricName.StdNumber.entryName -> (Math.sqrt(variance), None),
        MetricName.AvgNumber.entryName -> (mean, None)
      )
    }

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[StdAvgNumberRDDMetricCalculator]
      StdAvgNumberRDDMetricCalculator(
        this.sum + that.sum,
        this.sqSum + that.sqSum,
        this.cnt + that.cnt,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of elements that fit (or do not fit) provided Decimal format: Decimal(precision, scale)
   * @param cnt Current amount of elements which fit (didn't fit) decimal format
   * @param precision Precision threshold
   * @param scale Required scale
   * @param compareRule Either "inbound" or "outbound": defines wither number should fit
   *                    within provided decimal format or to be outside of the provided precision and scale
   * @return result map with keys: "FORMATTED_NUMBER"
   */
  case class FormattedNumberRDDMetricCalculator(cnt: Long,
                                                precision: Int,
                                                scale: Int,
                                                compareRule: String,
                                                protected val reversed: Boolean,
                                                protected val failCount: Long = 0,
                                                protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(precision: Int, scale: Int, compareRule: String, reversed: Boolean) =
      this(0, precision, scale, compareRule, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which do not meet provided precision
     * and scale criteria are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) FormattedNumberRDDMetricCalculator(
        cnt + rowCnt, precision, scale, compareRule, reversed, failCount
      )
      else FormattedNumberRDDMetricCalculator(
        cnt + rowCnt,
        precision,
        scale,
        compareRule,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        "Some of the provided values could not be cast to number which meets given" +
          s"precision and scale criteria of $criteriaStringRepr."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which DO meet provided precision
     * and scale criteria are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) FormattedNumberRDDMetricCalculator(
        cnt + rowCnt,
        precision,
        scale,
        compareRule,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        "Some of the provided values CAN be cast to number which meets given" +
          s"precision and scale criteria of $criteriaStringRepr."
      ) else FormattedNumberRDDMetricCalculator(cnt, precision, scale, compareRule, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.FormattedNumber.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[FormattedNumberRDDMetricCalculator]
      FormattedNumberRDDMetricCalculator(
        this.cnt + that.cnt,
        this.precision,
        this.scale,
        this.compareRule,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def compareFunc(value: Any): Boolean = tryToDouble(value).exists { v =>
      val bigD = BigDecimal.valueOf(v)
      compareRule match {
        case "inbound" => bigD.precision <= precision && bigD.scale <= scale
        case "outbound" => bigD.precision > precision && bigD.scale > scale
        case s => throw new IllegalArgumentException(s"Unknown compare rule for FORMATTED_NUMBER metric: '$s'")
      }
    }

    private def criteriaStringRepr: String = 
      if (compareRule == "inbound") s"(precision <= $precision; scale <= $scale)"
      else s"(precision > $precision; scale > $scale)" 
  }

  /**
   * Calculates amount of element that can be cast to numerical (double format)
   * @param cnt Current count of numeric elements
   *
   * @return result map with keys: "CASTED_NUMBER"
   */
  case class CastedNumberRDDMetricCalculator(cnt: Long,
                                             protected val reversed: Boolean,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(reversed: Boolean) = this(0, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that values which cannot be cast to number
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).length
      if (rowCnt == values.length) CastedNumberRDDMetricCalculator(cnt + rowCnt, reversed, failCount)
      else CastedNumberRDDMetricCalculator(
        cnt + rowCnt,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        "Some of the provided values cannot be cast to number."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that values which CAN be cast to number
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).length
      if (rowCnt > 0) CastedNumberRDDMetricCalculator(
        cnt + rowCnt,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        "Some of the provided values CAN be cast to number."
      ) else CastedNumberRDDMetricCalculator(cnt, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.CastedNumber.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[CastedNumberRDDMetricCalculator]
      CastedNumberRDDMetricCalculator(
        this.cnt + that.cnt,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculated amount of elements in provided domain set
   * @param cnt Current count of elements in domain
   * @param domain Set of element that represent requested domain
   * @return result map with keys: "NUMBER_IN_DOMAIN"
   */
  case class NumberInDomainRDDMetricCalculator(cnt: Long,
                                               domain: Set[Double],
                                               protected val reversed: Boolean,
                                               protected val failCount: Long = 0,
                                               protected val status: CalculatorStatus = CalculatorStatus.Success,
                                               protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(domain: Set[Double], reversed: Boolean) = this(0, domain, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which are outside of the provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt == values.length) NumberInDomainRDDMetricCalculator(
        cnt + rowCnt, domain, reversed, failCount
      )
      else NumberInDomainRDDMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are not in the given domain of ${domain.mkString("[", ",", "]")}."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which are IN the provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt > 0) NumberInDomainRDDMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are IN the given domain of ${domain.mkString("[", ",", "]")}."
      ) else NumberInDomainRDDMetricCalculator(cnt, domain, reversed, failCount)
    }


    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberInDomain.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NumberInDomainRDDMetricCalculator]
      NumberInDomainRDDMetricCalculator(
        this.cnt + that.cnt,
        this.domain,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of elements out of provided domain set
   * @param cnt Current count of elements out of domain
   * @param domain set of element that represent requested domain
   * @return result map with keys: "NUMBER_OUT_DOMAIN"
   */
  case class NumberOutDomainRDDMetricCalculator(cnt: Double,
                                                domain: Set[Double],
                                                protected val reversed: Boolean,
                                                protected val failCount: Long = 0,
                                                protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(domain: Set[Double], reversed: Boolean) = this(0, domain, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which are IN the provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      // takes into account non-number values also. these values are removed from sequence during flatMap operation
      val rowCnt = values.length - values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt == values.length) NumberOutDomainRDDMetricCalculator(
        cnt + rowCnt, domain, reversed, failCount
      )
      else NumberOutDomainRDDMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are IN the given domain of ${domain.mkString("[", ",", "]")}."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which are outside of the provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      // takes into account non-number values also. these values are removed from sequence during flatMap operation
      val rowCnt = values.length - values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt > 0) NumberOutDomainRDDMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are not in the given domain of ${domain.mkString("[", ",", "]")}."
      )
      else NumberOutDomainRDDMetricCalculator(cnt, domain, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberOutDomain.entryName -> (cnt, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NumberOutDomainRDDMetricCalculator]
      NumberOutDomainRDDMetricCalculator(
        this.cnt + that.cnt,
        this.domain,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Calculates count of requested value's appearance in processed elements
   * @param cnt Current count of appearance
   * @param compareValue Target value to track
   * @return result map with keys: "NUMBER_VALUES"
   */
  case class NumberValuesRDDMetricCalculator(cnt: Long,
                                             compareValue: Double,
                                             protected val reversed: Boolean,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(compareValue: Double, reversed: Boolean) = this(0, compareValue, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which are not equal to provided value
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(_ == compareValue)
      if (rowCnt == values.length) NumberValuesRDDMetricCalculator(
        cnt + rowCnt, compareValue, reversed, failCount
      )
      else NumberValuesRDDMetricCalculator(
        cnt + rowCnt,
        compareValue,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values do not equal to requested number value of '$compareValue'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which DO equal to provided value
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(_ == compareValue)
      if (rowCnt > 0) NumberValuesRDDMetricCalculator(
        cnt + rowCnt,
        compareValue,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values DO equal to requested number value of '$compareValue'."
      )
      else NumberValuesRDDMetricCalculator(cnt, compareValue, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberValues.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NumberValuesRDDMetricCalculator]
      NumberValuesRDDMetricCalculator(
        this.cnt + that.cnt,
        this.compareValue,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates count of rows for which column value is less than compareValue
   * @param cnt Current count of appearance
   * @param compareValue Target value to compare with
   * @param includeBound Flag which sets whether compareValue is included or excluded from the interval
   * @return result map with keys: "NUMBER_LESS_THAN"
   */
  case class NumberLessThanRDDMetricCalculator(cnt: Long,
                                               compareValue: Double,
                                               includeBound: Boolean,
                                               protected val reversed: Boolean,
                                               protected val failCount: Long = 0,
                                               protected val status: CalculatorStatus = CalculatorStatus.Success,
                                               protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(compareValue: Double, includeBound: Boolean, reversed: Boolean) =
      this(0, compareValue, includeBound, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which are greater than provided compareValue
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberLessThanRDDMetricCalculator(
        cnt + rowCnt, compareValue, includeBound, reversed, failCount
      )
      else NumberLessThanRDDMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '${if (includeBound) "<=" else "<"}$compareValue'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which are lower than provided compareValue
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberLessThanRDDMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '${if (includeBound) "<=" else "<"}$compareValue'."
      )
      else NumberLessThanRDDMetricCalculator(cnt, compareValue, includeBound, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberLessThan.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NumberLessThanRDDMetricCalculator]
      NumberLessThanRDDMetricCalculator(
        this.cnt + that.cnt,
        this.compareValue,
        this.includeBound,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def compareFunc(value: Any): Boolean = tryToDouble(value).exists { v =>
      if (includeBound) v <= compareValue else v < compareValue
    }
  }

  /**
   * Calculates count of rows for which column value is greater than compareValue
   * @param cnt Current count of appearance
   * @param compareValue Target value to compare with
   * @param includeBound Flag which sets whether compareValue is included (>=) or excluded (>) from the interval.
   * @return result map with keys: "NUMBER_GREATER_THAN"
   */
  case class NumberGreaterThanRDDMetricCalculator(cnt: Long,
                                                  compareValue: Double,
                                                  includeBound: Boolean,
                                                  protected val reversed: Boolean,
                                                  protected val failCount: Long = 0,
                                                  protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                  protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(compareValue: Double, includeBound: Boolean, reversed: Boolean) =
      this(0, compareValue, includeBound, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which are lower than provided compareValue
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberGreaterThanRDDMetricCalculator(
        cnt + rowCnt, compareValue, includeBound, reversed, failCount
      )
      else NumberGreaterThanRDDMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '${if (includeBound) ">=" else ">"}$compareValue'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which are greater than provided compareValue
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberGreaterThanRDDMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '${if (includeBound) ">=" else ">"}$compareValue'."
      ) else NumberGreaterThanRDDMetricCalculator(cnt, compareValue, includeBound, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberGreaterThan.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NumberGreaterThanRDDMetricCalculator]
      NumberGreaterThanRDDMetricCalculator(
        this.cnt + that.cnt,
        this.compareValue,
        this.includeBound,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def compareFunc(value: Any): Boolean = tryToDouble(value).exists { v =>
      if (includeBound) v >= compareValue else v > compareValue
    }
  }

  /**
   * Calculates count of rows for which column value is within the lowerCompareValue:upperCompareValue interval
   * @param cnt Current count of appearance
   * @param lowerCompareValue Target lower interval bound to compare with
   * @param upperCompareValue Target upper interval bound to compare with
   * @param includeBound Flag which sets whether interval bounds are included or excluded from the interval.
   * @return result map with keys: "NUMBER_BETWEEN"
   */
  case class NumberBetweenRDDMetricCalculator(cnt: Long,
                                              lowerCompareValue: Double,
                                              upperCompareValue: Double,
                                              includeBound: Boolean,
                                              protected val reversed: Boolean,
                                              protected val failCount: Long = 0,
                                              protected val status: CalculatorStatus = CalculatorStatus.Success,
                                              protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(lowerCompareValue: Double, upperCompareValue: Double, includeBound: Boolean, reversed: Boolean) =
      this(0, lowerCompareValue, upperCompareValue, includeBound, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which are outside of provided interval
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberBetweenRDDMetricCalculator(
        cnt + rowCnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
      else NumberBetweenRDDMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '$criteriaStringRepr'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which are inside of the provided interval
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberBetweenRDDMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '$criteriaStringRepr'."
      ) else NumberBetweenRDDMetricCalculator(
        cnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberBetween.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NumberBetweenRDDMetricCalculator]
      NumberBetweenRDDMetricCalculator(
        this.cnt + that.cnt,
        this.lowerCompareValue,
        this.upperCompareValue,
        this.includeBound,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def compareFunc(value: Any): Boolean = tryToDouble(value).exists { v =>
      if (includeBound) v >= lowerCompareValue && v <= upperCompareValue 
      else v > lowerCompareValue && v < upperCompareValue
    }

    private def criteriaStringRepr: String =
      if (includeBound) s">=$lowerCompareValue AND <=$upperCompareValue"
      else s">$lowerCompareValue AND <$upperCompareValue"
  }

  /**
   * Calculates count of rows for which column value is not within the lowerCompareValue:upperCompareValue interval
   * @param cnt Current count of appearance
   * @param lowerCompareValue Target lower interval bound to compare with
   * @param upperCompareValue Target upper interval bound to compare with
   * @param includeBound Flag which sets whether interval bounds are included or excluded from the interval.
   * @return result map with keys: "NUMBER_NOT_BETWEEN"
   */
  case class NumberNotBetweenRDDMetricCalculator(cnt: Long,
                                                 lowerCompareValue: Double,
                                                 upperCompareValue: Double,
                                                 includeBound: Boolean,
                                                 protected val reversed: Boolean,
                                                 protected val failCount: Long = 0,
                                                 protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                 protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(lowerCompareValue: Double, upperCompareValue: Double, includeBound: Boolean, reversed: Boolean) =
      this(0, lowerCompareValue, upperCompareValue, includeBound, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that numeric values which are inside of provided interval
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberNotBetweenRDDMetricCalculator(
        cnt + rowCnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
      else NumberNotBetweenRDDMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '$criteriaStringRepr'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that numeric values which are outside of the provided interval
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberNotBetweenRDDMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '$criteriaStringRepr'."
      ) else NumberNotBetweenRDDMetricCalculator(
        cnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberNotBetween.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NumberNotBetweenRDDMetricCalculator]
      NumberNotBetweenRDDMetricCalculator(
        this.cnt + that.cnt,
        this.lowerCompareValue,
        this.upperCompareValue,
        this.includeBound,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def compareFunc(value: Any): Boolean = tryToDouble(value).exists { v =>
      if (includeBound) v <= lowerCompareValue || v >= upperCompareValue
      else v < lowerCompareValue || v > upperCompareValue
    }

    private def criteriaStringRepr: String =
      if (includeBound) s"<=$lowerCompareValue OR >=$upperCompareValue"
      else s"<$lowerCompareValue OR >$upperCompareValue"
  }

  /**
   * Calculates completeness of incremental integer (long) sequence,
   * i.e. checks if sequence does not have missing elements.
   * Works for single column only!
   * @param minVal Minimum observed value in a sequence
   * @param maxVal Maximum observed value in a sequence
   * @param increment Sequence increment
   * @param uniqueValues Set of unique values in a sequence
   * @return result map with keys: "SEQUENCE_COMPLETENESS"
   */
  case class SequenceCompletenessRDDMetricCalculator(uniqueValues: Set[Long],
                                                     increment: Long,
                                                     minVal: Long,
                                                     maxVal: Long,
                                                     protected val failCount: Long = 0,
                                                     protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                     protected val failMsg: String = "OK") extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this(increment: Long) = this(Set.empty[Long], increment, Long.MaxValue, Long.MinValue)
      
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      assert(values.length == 1, "sequenceCompleteness metric works with single column only!")
      tryToLong(values.head) match {
        case Some(value) => SequenceCompletenessRDDMetricCalculator(
          uniqueValues + value, increment, Math.min(minVal, value), Math.max(maxVal, value), failCount
        )
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Provided value cannot be cast to number."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] = Map(
      MetricName.SequenceCompleteness.entryName -> 
        (uniqueValues.size.toDouble / ((maxVal - minVal).toDouble / increment.toDouble + 1.0), None)
    )

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[SequenceCompletenessRDDMetricCalculator]
      SequenceCompletenessRDDMetricCalculator(
        this.uniqueValues ++ that.uniqueValues,
        this.increment,
        Math.min(this.minVal, that.minVal),
        Math.max(this.maxVal, that.maxVal),
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }
}

