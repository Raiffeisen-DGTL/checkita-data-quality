package ru.raiffeisen.checkita.metrics.column

import org.isarnproject.sketches.TDigest
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.metrics.CalculatorStatus.CalculatorStatus
import ru.raiffeisen.checkita.metrics.{CalculatorStatus, MetricCalculator, StatusableCalculator}
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.utils.{getParametrizedMetricTail, tryToDouble, tryToLong}

import scala.util.{Failure, Success, Try}

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
   * @param paramMap Required configuration map. May contains:
   *   "accuracy error" - required level of calculation accuracy. By default = 0.005
   *   "targetSideNumber" - required parameter
   *       For quantiles should be in [0,1]
   *
   * @return result map with keys:
   *   "GET_QUANTILE"
   *   "GET_PERCENTILE"
   *   "FIRST_QUANTILE"
   *   "THIRD_QUANTILE"
   *   "MEDIAN_VALUE"
   */
  case class TDigestMetricCalculator(tdigest: TDigest, paramMap: ParamMap)
    extends MetricCalculator {

    def this(paramMap: ParamMap) = {
      this(TDigest.empty(
        paramMap.getOrElse("accuracyError", 0.005).toString.toDouble),
        paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "TDigest metrics work for single column only!")
      tryToDouble(values.head) match {
        case Some(v) => TDigestMetricCalculator(tdigest + v, paramMap)
        case None    => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] = {

      val requestedResult: Double =
        paramMap.getOrElse("targetSideNumber", 0).toString.toDouble

      val staticResults: Map[String, (Double, Option[String])] = Map(
        "MEDIAN_VALUE" + getParametrizedMetricTail(paramMap) -> (tdigest
          .cdfInverse(0.5), None),
        "FIRST_QUANTILE" + getParametrizedMetricTail(paramMap) -> (tdigest
          .cdfInverse(0.25), None),
        "THIRD_QUANTILE" + getParametrizedMetricTail(paramMap) -> (tdigest
          .cdfInverse(0.75), None)
      )

      // todo refactor after parameter grouping
      val parametrizedResults: Map[String, (Double, Option[String])] =
        requestedResult match {
          case x if x >= 0 && x <= 1 =>
            Map(
              "GET_QUANTILE" + getParametrizedMetricTail(paramMap) -> (tdigest
                .cdfInverse(x), None),
              "GET_PERCENTILE" + getParametrizedMetricTail(paramMap) -> (tdigest
                .cdf(x), None)
            )
          case x =>
            Map(
              "GET_PERCENTILE" + getParametrizedMetricTail(paramMap) -> (tdigest
                .cdf(x), None)
            )
        }

      staticResults ++ parametrizedResults
    }

    override def merge(m2: MetricCalculator): MetricCalculator =
      TDigestMetricCalculator(
        tdigest ++ m2.asInstanceOf[TDigestMetricCalculator].tdigest,
        paramMap)

  }

  /**
   * Calculates minimal value for provided elements
   * @param min Current minimal value
   *
   * @return result map with keys:
   *   "MIN_NUMBER"
   */
  case class MinNumericValueMetricCalculator(min: Double)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(Double.MaxValue)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      // .min will throw "UnsupportedOperationException" when applied to empty sequence.
      val rowMinValue = Try(values.flatMap(tryToDouble).min).toOption
      rowMinValue match {
        case Some(v) => MinNumericValueMetricCalculator(Math.min(v, min))
        case None    => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("MIN_NUMBER" -> (min, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      MinNumericValueMetricCalculator(
        Math.min(this.min,
          m2.asInstanceOf[MinNumericValueMetricCalculator].min))

  }

  /**
   * Calculates maximal value for provided elements
   * @param max Current maximal value
   *
   * @return result map with keys:
   *   "MAX_NUMBER"
   */
  case class MaxNumericValueMetricCalculator(max: Double)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(Double.MinValue)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      // .max will throw "UnsupportedOperationException" when applied to empty sequence.
      val rowMaxValue = Try(values.flatMap(tryToDouble).max).toOption
      rowMaxValue match {
        case Some(v) => MaxNumericValueMetricCalculator(Math.max(v, max))
        case None    => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("MAX_NUMBER" -> (max, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      MaxNumericValueMetricCalculator(
        Math.max(this.max,
          m2.asInstanceOf[MaxNumericValueMetricCalculator].max))

  }

  /**
   * Calculates sum of provided elements
   * @param sum Current sum
   *
   * @return result map with keys:
   *   "SUM_NUMBER"
   */
  case class SumNumericValueMetricCalculator(sum: Double)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = SumNumericValueMetricCalculator(
      sum + values.flatMap(tryToDouble).sum
    )

    override def result(): Map[String, (Double, Option[String])] =
      Map("SUM_NUMBER" -> (sum.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      SumNumericValueMetricCalculator(
        this.sum + m2.asInstanceOf[SumNumericValueMetricCalculator].sum)

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
  case class StdAvgNumericValueCalculator(sum: Double, sqSum: Double, cnt: Int)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0, 0, 0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "avgNumber and stdNumber metrics work for single column only!")
      tryToDouble(values.head) match {
        case Some(v) =>
          StdAvgNumericValueCalculator(sum + v, sqSum + (v * v), cnt + 1)
        case None => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] = {
      val mean = sum / cnt.toDouble
      val variance = sqSum / cnt.toDouble - mean * mean
      Map(
        "STD_NUMBER" -> (Math.sqrt(variance), None),
        "AVG_NUMBER" -> (mean, None)
      )
    }

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val cm2 = m2.asInstanceOf[StdAvgNumericValueCalculator]
      StdAvgNumericValueCalculator(
        this.sum + cm2.sum,
        this.sqSum + cm2.sqSum,
        this.cnt + cm2.cnt
      )
    }
  }

  /**
   * Calculates amount of elements that fit (or do not fit) provided Decimal format: Decimal(precision, scale)
   * @param cnt Current amount of elements which fit (didn't fit) decimal format
   * @param paramMap Required configuration map. May contains:
   *
   *   required "precision" - precision threshold
   *
   *   required "scale" - required scale
   *
   *   optional "compareRule" - either "inbound" or "outbound": defines wither number should fit
   *   within provided decimal format or to be outside of the provided precision and scale
   *   (default is "inbound")
   *
   * @return result map with keys:
   *   "FORMATTED_NUMBER"
   */
  case class NumberFormattedValuesMetricCalculator(cnt: Double,
                                                   paramMap: ParamMap,
                                                   protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                   protected val failCount: Int = 0)
    extends StatusableCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    private val precision: Int = paramMap("precision").toString.toInt
    private val scale: Int = paramMap("scale").toString.toInt

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.count(x => checkNumber(compareFunc)(x, precision, scale))
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberFormattedValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("FORMATTED_NUMBER" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberFormattedValuesMetricCalculator]
      NumberFormattedValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

    private val compareFunc: (Int, Int) => Boolean = Try(paramMap("compareRule").toString) match {
      case Failure(_) => (value, threshold) => value <= threshold // default
      case Success(s) => s match {
        case "inbound" => (value, threshold) => value <= threshold
        case "outbound" => (value, threshold) => value > threshold
        case s => throw IllegalParameterException(s"Unknown rule for FORMATTED_NUMBER metric: $s")
      }
    }

    private def checkNumber(compareFunc: (Int, Int) => Boolean)
                           (value: Any, precision: Int, scale: Int): Boolean = tryToDouble(value) match {
      case Some(d) =>
        val bigD = BigDecimal.valueOf(d)
        compareFunc(bigD.precision, precision) && compareFunc(bigD.scale, scale)
      case None => false
    }
  }

  /**
   * Calculates amount of element that can be custed to numerical (double format)
   * @param cnt Current count of custabale elements
   *
   * @return result map with keys:
   *   "CASTED_NUMBER"
   */
  case class NumberCastValuesMetricCalculator(cnt: Double,
                                              protected val status: CalculatorStatus = CalculatorStatus.OK,
                                              protected val failCount: Int = 0)
    extends StatusableCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).length
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberCastValuesMetricCalculator(
        cnt + rowCnt,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("CASTED_NUMBER" -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberCastValuesMetricCalculator]
      NumberCastValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }

  /**
   * Calculated amount of elements in provided domain set
   * @param cnt Current count of elements in domain
   * @param paramMap Required configuration map. May contains:
   *   required "domain" - set of element that represent requested domain

   * @return result map with keys:
   *   "NUMBER_IN_DOMAIN"
   */
  case class NumberInDomainValuesMetricCalculator(cnt: Double,
                                                  paramMap: ParamMap,
                                                  protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                  protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val domain = paramMap("domain").asInstanceOf[List[Double]].toSet

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberInDomainValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "NUMBER_IN_DOMAIN" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberInDomainValuesMetricCalculator]
      NumberInDomainValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }

  /**
   * Calculates amount of elements out of provided domain set
   * @param cnt Current count of elements out of domain
   * @param paramMap Required configuration map. May contains:
   *   required "domain" - set of element that represent requested domain

   * @return result map with keys:
   *   "NUMBER_OUT_DOMAIN"
   */
  case class NumberOutDomainValuesMetricCalculator(cnt: Double,
                                                   paramMap: ParamMap,
                                                   protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                   protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val domain = paramMap("domain").asInstanceOf[List[Double]].toSet

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      // takes into account non-number values also. these values are removed from sequence during flatMap operation
      val rowCnt = values.length - values.flatMap(tryToDouble).count(domain.contains)
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberOutDomainValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("NUMBER_OUT_DOMAIN" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberOutDomainValuesMetricCalculator]
      NumberOutDomainValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

  }

  /**
   * Calculates count of requested value's appearance in processed elements
   * @param cnt Current count of appearance
   * @param paramMap Required configuration map. May contains:
   *   required "compareValue" - target value to track

   * @return result map with keys:
   *   "NUMBER_VALUES"
   */
  case class NumberValuesMetricCalculator(cnt: Int,
                                          paramMap: ParamMap,
                                          protected val status: CalculatorStatus = CalculatorStatus.OK,
                                          protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val lvalue: Double = paramMap("compareValue").toString.toDouble

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(_ == lvalue)
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "NUMBER_VALUES" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberValuesMetricCalculator]
      NumberValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }

  /**
   * Calculates count of rows for which column value is less than compareValue
   * @param cnt Current count of appearance
   * @param paramMap Required configuration map. May contains:
   *   required "compareValue" - target value to compare with
   *   optional "includeBound" - flag which sets whether compareValue is included or excluded from the interval.
   *                             (if omitted then compareValue is excluded from the interval)
   * @return result map with keys:
   *   "NUMBER_LESS_THAN"
   */
  case class NumberLessThanMetricCalculator(cnt: Int,
                                            paramMap: ParamMap,
                                            protected val status: CalculatorStatus = CalculatorStatus.OK,
                                            protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val uvalue: Double = paramMap("compareValue").toString.toDouble
    private val includeBound: Boolean = Try(paramMap("includeBound").toString.toBoolean).getOrElse(false)

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(x => compareFunc(includeBound)(x, uvalue))
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberLessThanMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "NUMBER_LESS_THAN" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberLessThanMetricCalculator]
      NumberLessThanMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        failCount + m2casted.getFailCounter
      )
    }

    private def compareFunc(includeBound: Boolean): (Double, Double) => Boolean = {
      if (includeBound) (value, threshold) => value <= threshold
      else (value, threshold) => value < threshold
    }
  }

  /**
   * Calculates count of rows for which column value is greater than compareValue
   * @param cnt Current count of appearance
   * @param paramMap Required configuration map. May contains:
   *   required "compareValue" - target value to compare with
   *   optional "includeBound" - flag which sets whether compareValue is included (>=) or excluded (>) from the interval.
   *                             (if omitted then compareValue is excluded from the interval)
   * @return result map with keys:
   *   "NUMBER_GREATER_THAN"
   */
  case class NumberGreaterThanMetricCalculator(cnt: Int,
                                               paramMap: ParamMap,
                                               protected val status: CalculatorStatus = CalculatorStatus.OK,
                                               protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val lvalue: Double = paramMap("compareValue").toString.toDouble
    private val includeBound: Boolean = Try(paramMap("includeBound").toString.toBoolean).getOrElse(false)

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(x => compareFunc(includeBound)(x, lvalue))
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberGreaterThanMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "NUMBER_GREATER_THAN" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberGreaterThanMetricCalculator]
      NumberGreaterThanMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

    private def compareFunc(includeBound: Boolean): (Double, Double) => Boolean = {
      if (includeBound) (value, threshold) => value >= threshold
      else (value, threshold) => value > threshold
    }
  }

  /**
   * Calculates count of rows for which column value is within the lowerCompareValue:upperCompareValue interval
   * @param cnt Current count of appearance
   * @param paramMap Required configuration map. May contains:
   *   required "lowerCompareValue" - target lower interval bound to compare with
   *   required "upperCompareValue" - target upper interval bound to compare with
   *   optional "includeBound" - flag which sets whether interval bounds are included or excluded from the interval.
   *                             (if omitted then interval bounds are excluded from the interval)
   * @return result map with keys:
   *   "NUMBER_BETWEEN"
   */
  case class NumberBetweenMetricCalculator(cnt: Int,
                                           paramMap: ParamMap,
                                           protected val status: CalculatorStatus = CalculatorStatus.OK,
                                           protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val lvalue: Double = paramMap("lowerCompareValue").toString.toDouble
    private val uvalue: Double = paramMap("upperCompareValue").toString.toDouble
    private val includeBound: Boolean = Try(paramMap("includeBound").toString.toBoolean).getOrElse(false)

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(x => compareFunc(includeBound)(x, lvalue, uvalue))
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberBetweenMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "NUMBER_BETWEEN" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberBetweenMetricCalculator]
      NumberBetweenMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

    private def compareFunc(includeBound: Boolean): (Double, Double, Double) => Boolean = {
      if (includeBound) (value, thresholdLower, thresholdUpper) => value >= thresholdLower && value <= thresholdUpper
      else (value, thresholdLower, thresholdUpper) => value > thresholdLower && value < thresholdUpper
    }
  }

  /**
   * Calculates count of rows for which column value is not within the lowerCompareValue:upperCompareValue interval
   *
   * @param cnt      Current count of appearance
   * @param paramMap Required configuration map. May contains:
   *                 required "lowerCompareValue" - target lower interval bound to compare with
   *                 required "upperCompareValue" - target upper interval bound to compare with
   *                 optional "includeBound" - flag which sets whether interval bounds are included or excluded from the interval.
   *                 (if omitted then interval bounds are excluded from the interval)
   * @return result map with keys:
   *         "NUMBER_NOT_BETWEEN"
   */
  case class NumberNotBetweenMetricCalculator(cnt: Int,
                                              paramMap: ParamMap,
                                              protected val status: CalculatorStatus = CalculatorStatus.OK,
                                              protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val lvalue: Double = paramMap("lowerCompareValue").toString.toDouble
    private val uvalue: Double = paramMap("upperCompareValue").toString.toDouble
    private val includeBound: Boolean = Try(paramMap("includeBound").toString.toBoolean).getOrElse(false)

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToDouble).count(x => compareFunc(includeBound)(x, lvalue, uvalue))
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else NumberNotBetweenMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "NUMBER_NOT_BETWEEN" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[NumberNotBetweenMetricCalculator]
      NumberNotBetweenMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

    private def compareFunc(includeBound: Boolean): (Double, Double, Double) => Boolean = {
      if (includeBound) (value, thresholdLower, thresholdUpper) => value <= thresholdLower || value >= thresholdUpper
      else (value, thresholdLower, thresholdUpper) => value < thresholdLower || value > thresholdUpper
    }
  }

  /**
   * Calculates completeness of incremental integer (long) sequence,
   * i.e. checks if sequence does not have missing elements.
   * Works for single column only!
   * @param minVal - minimum observed value in a sequence
   * @param maxVal - maximum observed value in a sequence
   * @param uniqueValues - set of unique values in a sequence
   * @param paramMap Optional configuration map. May contains:
   *                 optional key "increment" - sequence increment. Default: 1
   * @return result map with keys:
   *         "SEQUENCE_COMPLETENESS"
   */
  case class SequenceCompletenessMetricCalculator(minVal: Long,
                                                  maxVal: Long,
                                                  uniqueValues: Set[Long],
                                                  paramMap: ParamMap) extends MetricCalculator {

    private val seqInc: Long = Try(paramMap("increment").toString.toLong).getOrElse(1)

    def this(paramMap: Map[String, Any]) {
      this(Long.MaxValue, Long.MinValue, Set.empty[Long], paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "sequenceCompleteness metric works with single column only!")
      tryToLong(values.head) match {
        case Some(value) => SequenceCompletenessMetricCalculator(
          Math.min(minVal, value), Math.max(maxVal, value), uniqueValues + value, paramMap
        )
        case None => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("SEQUENCE_COMPLETENESS" -> (uniqueValues.size.toDouble / ((maxVal - minVal) / seqInc + 1), None))

    override def merge(m2: MetricCalculator): MetricCalculator = SequenceCompletenessMetricCalculator(
      Math.min(this.minVal, m2.asInstanceOf[SequenceCompletenessMetricCalculator].minVal),
      Math.max(this.maxVal, m2.asInstanceOf[SequenceCompletenessMetricCalculator].maxVal),
      this.uniqueValues ++ m2.asInstanceOf[SequenceCompletenessMetricCalculator].uniqueValues,
      paramMap
    )
  }
}
