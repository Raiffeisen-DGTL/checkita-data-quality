package ru.raiffeisen.checkita.core.metrics.regular

import org.isarnproject.sketches.TDigest
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Casting.{tryToDouble, tryToLong}
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName, ReversibleCalculator}

import scala.util.Try

/**
 * Basic metrics that can be applied to numerical elements
 */
object BasicNumericMetrics {

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
  case class TDigestMetricCalculator(tdigest: TDigest,
                                     accuracyError: Double,
                                     targetSideNumber: Double,
                                     protected val failCount: Long = 0,
                                     protected val status: CalculatorStatus = CalculatorStatus.Success,
                                     protected val failMsg: String = "OK")
    extends MetricCalculator {
    
    // axillary constructor to initiate with empty TDigest object
    def this(accuracyError: Double, targetSideNumber: Double) =
      this(TDigest.empty(accuracyError), accuracyError, targetSideNumber)

    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "TDigest metrics work for single column only!")
      tryToDouble(values.head) match {
        case Some(v) => TDigestMetricCalculator(
          tdigest + v, accuracyError, targetSideNumber, failCount
        )
        case None    => copyWithError(CalculatorStatus.Failure,
          "Provided value cannot be cast to a number"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
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

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[TDigestMetricCalculator]
      TDigestMetricCalculator(
        this.tdigest ++ that.tdigest,
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
  case class MinNumericValueMetricCalculator(min: Double,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Double.MaxValue)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      // .min will throw "UnsupportedOperationException" when applied to empty sequence.
      val rowMinValue = Try(values.flatMap(tryToDouble).min).toOption
      rowMinValue match {
        case Some(v) => MinNumericValueMetricCalculator(Math.min(v, min), failCount)
        case None    => copyWithError(
          CalculatorStatus.Failure,
          "Couldn't compute minimum number out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MinNumber.entryName -> (min, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[MinNumericValueMetricCalculator]
      MinNumericValueMetricCalculator(
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
  case class MaxNumericValueMetricCalculator(max: Double,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Double.MinValue)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      // .max will throw "UnsupportedOperationException" when applied to empty sequence.
      val rowMaxValue = Try(values.flatMap(tryToDouble).max).toOption
      rowMaxValue match {
        case Some(v) => MaxNumericValueMetricCalculator(Math.max(v, max), failCount)
        case None    => copyWithError(
          CalculatorStatus.Failure,
          "Couldn't compute maximum number out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MaxNumber.entryName -> (max, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[MaxNumericValueMetricCalculator]
      MaxNumericValueMetricCalculator(
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
  case class SumNumericValueMetricCalculator(sum: Double,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = { 
      val doubleValues = values.flatMap(tryToDouble)
      if (doubleValues.size == values.size) SumNumericValueMetricCalculator(sum + doubleValues.sum, failCount)
      else SumNumericValueMetricCalculator(
        sum + doubleValues.sum,
        failCount + values.size - doubleValues.size,
        CalculatorStatus.Failure,
        "Some of the provided values cannot be cast to number"
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] = 
      Map(MetricName.SumNumber.entryName -> (sum, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[SumNumericValueMetricCalculator]
      SumNumericValueMetricCalculator(
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
  case class StdAvgNumericValueCalculator(sum: Double,
                                          sqSum: Double,
                                          cnt: Long,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, 0, 0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "avgNumber and stdNumber metrics work for single column only!")
      tryToDouble(values.head) match {
        case Some(v) => StdAvgNumericValueCalculator(sum + v, sqSum + (v * v), cnt + 1, failCount)
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Provided value cannot be cast to number"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] = {
      val mean = sum / cnt.toDouble
      val variance = sqSum / cnt.toDouble - mean * mean
      Map(
        MetricName.StdNumber.entryName -> (Math.sqrt(variance), None),
        MetricName.AvgNumber.entryName -> (mean, None)
      )
    }

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[StdAvgNumericValueCalculator]
      StdAvgNumericValueCalculator(
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
  case class NumberFormattedValuesMetricCalculator(cnt: Long,
                                                   precision: Int,
                                                   scale: Int,
                                                   compareRule: String,
                                                   protected val reversed: Boolean,
                                                   protected val failCount: Long = 0,
                                                   protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                   protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberFormattedValuesMetricCalculator(
        cnt + rowCnt, precision, scale, compareRule, reversed, failCount
      )
      else NumberFormattedValuesMetricCalculator(
        cnt + rowCnt,
        precision,
        scale,
        compareRule,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        "Some of the provided values could not be cast to number which meets given" + 
          s"precision and scale criteria of $criteriaStringRepr"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberFormattedValuesMetricCalculator(
        cnt + rowCnt,
        precision,
        scale,
        compareRule,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        "Some of the provided values CAN be cast to number which meets given" +
          s"precision and scale criteria of $criteriaStringRepr"
      ) else NumberFormattedValuesMetricCalculator(cnt, precision, scale, compareRule, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.FormattedNumber.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberFormattedValuesMetricCalculator]
      NumberFormattedValuesMetricCalculator(
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
  case class NumberCastValuesMetricCalculator(cnt: Long,
                                              protected val reversed: Boolean,
                                              protected val failCount: Long = 0,
                                              protected val status: CalculatorStatus = CalculatorStatus.Success,
                                              protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).length
      if (rowCnt == values.length) NumberCastValuesMetricCalculator(cnt + rowCnt, reversed, failCount)
      else NumberCastValuesMetricCalculator(
        cnt + rowCnt,
        reversed,
        failCount + values.length - rowCnt,
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).length
      if (rowCnt > 0) NumberCastValuesMetricCalculator(
        cnt + rowCnt,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        "Some of the provided values CAN be cast to number"
      ) else NumberCastValuesMetricCalculator(cnt, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.CastedNumber.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberCastValuesMetricCalculator]
      NumberCastValuesMetricCalculator(
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
  case class NumberInDomainValuesMetricCalculator(cnt: Long,
                                                  domain: Set[Double],
                                                  protected val reversed: Boolean,
                                                  protected val failCount: Long = 0,
                                                  protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                  protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt == values.length) NumberInDomainValuesMetricCalculator(
        cnt + rowCnt, domain, reversed, failCount
      )
      else NumberInDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are not in the provided domain of ${domain.mkString("[", ",", "]")}"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt > 0) NumberInDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are IN the provided domain of ${domain.mkString("[", ",", "]")}"
      ) else NumberInDomainValuesMetricCalculator(cnt, domain, reversed, failCount)
    }


    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberInDomain.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberInDomainValuesMetricCalculator]
      NumberInDomainValuesMetricCalculator(
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
  case class NumberOutDomainValuesMetricCalculator(cnt: Double,
                                                   domain: Set[Double],
                                                   protected val reversed: Boolean,
                                                   protected val failCount: Long = 0,
                                                   protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                   protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      // takes into account non-number values also. these values are removed from sequence during flatMap operation
      val rowCnt = values.length - values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt == values.length) NumberOutDomainValuesMetricCalculator(
        cnt + rowCnt, domain, reversed, failCount
      )
      else NumberOutDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are IN the provided domain of ${domain.mkString("[", ",", "]")}"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      // takes into account non-number values also. these values are removed from sequence during flatMap operation
      val rowCnt = values.length - values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt > 0) NumberOutDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided numeric values are outside of the provided domain of ${domain.mkString("[", ",", "]")}"
      )
      else NumberOutDomainValuesMetricCalculator(cnt, domain, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberOutDomain.entryName -> (cnt, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberOutDomainValuesMetricCalculator]
      NumberOutDomainValuesMetricCalculator(
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
  case class NumberValuesMetricCalculator(cnt: Long,
                                          compareValue: Double,
                                          protected val reversed: Boolean,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(_ == compareValue)
      if (rowCnt == values.length) NumberValuesMetricCalculator(
        cnt + rowCnt, compareValue, reversed, failCount
      )
      else NumberValuesMetricCalculator(
        cnt + rowCnt,
        compareValue,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values do not equal to requested number value of '$compareValue'"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(_ == compareValue)
      if (rowCnt > 0) NumberValuesMetricCalculator(
        cnt + rowCnt,
        compareValue,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values DO equal to requested number value of '$compareValue'"
      )
      else NumberValuesMetricCalculator(cnt, compareValue, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberValues.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberValuesMetricCalculator]
      NumberValuesMetricCalculator(
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
  case class NumberLessThanMetricCalculator(cnt: Long,
                                            compareValue: Double,
                                            includeBound: Boolean,
                                            protected val reversed: Boolean,
                                            protected val failCount: Long = 0,
                                            protected val status: CalculatorStatus = CalculatorStatus.Success,
                                            protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberLessThanMetricCalculator(
        cnt + rowCnt, compareValue, includeBound, reversed, failCount
      )
      else NumberLessThanMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '${if (includeBound) "<=" else "<"}$compareValue'"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberLessThanMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '${if (includeBound) "<=" else "<"}$compareValue'"
      )
      else NumberLessThanMetricCalculator(cnt, compareValue, includeBound, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberLessThan.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberLessThanMetricCalculator]
      NumberLessThanMetricCalculator(
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
  case class NumberGreaterThanMetricCalculator(cnt: Long,
                                               compareValue: Double,
                                               includeBound: Boolean,
                                               protected val reversed: Boolean,
                                               protected val failCount: Long = 0,
                                               protected val status: CalculatorStatus = CalculatorStatus.Success,
                                               protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberGreaterThanMetricCalculator(
        cnt + rowCnt, compareValue, includeBound, reversed, failCount
      )
      else NumberGreaterThanMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '${if (includeBound) ">=" else ">"}$compareValue'"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberGreaterThanMetricCalculator(
        cnt + rowCnt,
        compareValue,
        includeBound,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '${if (includeBound) ">=" else ">"}$compareValue'"
      ) else NumberGreaterThanMetricCalculator(cnt, compareValue, includeBound, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberGreaterThan.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberGreaterThanMetricCalculator]
      NumberGreaterThanMetricCalculator(
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
  case class NumberBetweenMetricCalculator(cnt: Long,
                                           lowerCompareValue: Double,
                                           upperCompareValue: Double,
                                           includeBound: Boolean,
                                           protected val reversed: Boolean,
                                           protected val failCount: Long = 0,
                                           protected val status: CalculatorStatus = CalculatorStatus.Success,
                                           protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberBetweenMetricCalculator(
        cnt + rowCnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
      else NumberBetweenMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '$criteriaStringRepr'"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberBetweenMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '$criteriaStringRepr'"
      ) else NumberBetweenMetricCalculator(
        cnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberBetween.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberBetweenMetricCalculator]
      NumberBetweenMetricCalculator(
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
  case class NumberNotBetweenMetricCalculator(cnt: Long,
                                              lowerCompareValue: Double,
                                              upperCompareValue: Double,
                                              includeBound: Boolean,
                                              protected val reversed: Boolean,
                                              protected val failCount: Long = 0,
                                              protected val status: CalculatorStatus = CalculatorStatus.Success,
                                              protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt == values.length) NumberNotBetweenMetricCalculator(
        cnt + rowCnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
      else NumberNotBetweenMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet numeric criteria of '$criteriaStringRepr'"
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
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(compareFunc)
      if (rowCnt > 0) NumberNotBetweenMetricCalculator(
        cnt + rowCnt,
        lowerCompareValue,
        upperCompareValue,
        includeBound,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values DO meet numeric criteria of '$criteriaStringRepr'"
      ) else NumberNotBetweenMetricCalculator(
        cnt, lowerCompareValue, upperCompareValue, includeBound, reversed, failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NumberNotBetween.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NumberNotBetweenMetricCalculator]
      NumberNotBetweenMetricCalculator(
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
      if (includeBound) s"<=$lowerCompareValue AND >=$upperCompareValue"
      else s"<$lowerCompareValue AND >$upperCompareValue"
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
  case class SequenceCompletenessMetricCalculator(uniqueValues: Set[Long],
                                                  increment: Long,
                                                  minVal: Long,
                                                  maxVal: Long,
                                                  protected val failCount: Long = 0,
                                                  protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                  protected val failMsg: String = "OK") extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(increment: Long) = this(Set.empty[Long], increment, Long.MaxValue, Long.MinValue)
      
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "sequenceCompleteness metric works with single column only!")
      tryToLong(values.head) match {
        case Some(value) => SequenceCompletenessMetricCalculator(
          uniqueValues + value, increment, Math.min(minVal, value), Math.max(maxVal, value), failCount
        )
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Provided value cannot be cast to a number"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] = Map(
      MetricName.SequenceCompleteness.entryName -> 
        (uniqueValues.size.toDouble / ((maxVal - minVal).toDouble / increment.toDouble + 1.0), None)
    )

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[SequenceCompletenessMetricCalculator]
      SequenceCompletenessMetricCalculator(
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

