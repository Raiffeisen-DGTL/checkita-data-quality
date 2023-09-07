package ru.raiffeisen.checkita.metrics.column

import ru.raiffeisen.checkita.metrics.CalculatorStatus.CalculatorStatus
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.metrics.{CalculatorStatus, MetricCalculator, StatusableCalculator}
import ru.raiffeisen.checkita.utils.{getParametrizedMetricTail, tryToDate, tryToString}

import scala.util.Try

/**
 * Basic metrics that can be applied to string (or string like) elements
 */
object BasicStringMetrics {

  /**
   * Calculates count of distinct values in processed elements
   * WARNING: Uses set without any kind of trimming and hashing. Return the exact count.
   * So if you a big diversion of elements and does not need an exact result,
   * it's better to use HyperLogLog version (called with "APPROXIMATE_DISTINCT_VALUES").
   *
   * @param uniqueValues Set of processed values
   * @return result map with keys:
   *         "DISTINCT_VALUES"
   */
  case class UniqueValuesMetricCalculator(uniqueValues: Set[Any])
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(Set.empty[Any])
    }

    override def increment(values: Seq[Any]): MetricCalculator = UniqueValuesMetricCalculator(
      uniqueValues ++ values.flatMap(tryToString).toSet
    )


    override def result(): Map[String, (Double, Option[String])] =
      Map("DISTINCT_VALUES" -> (uniqueValues.size.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      UniqueValuesMetricCalculator(
        this.uniqueValues ++ m2
          .asInstanceOf[UniqueValuesMetricCalculator]
          .uniqueValues)

  }

  /**
   * Calculates amount of rows that match the provided regular expression
   *
   * @param cnt      current counter
   * @param paramMap should contain regex
   */
  case class RegexMatchMetricCalculator(cnt: Int,
                                        paramMap: Map[String, Any],
                                        protected val status: CalculatorStatus = CalculatorStatus.OK,
                                        protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val regex: String = paramMap("regex").toString

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val matchCnt: Int = values.count(elem => tryToString(elem) match {
        case Some(x) => x.matches(regex)
        case None => false
      })
      if (matchCnt == values.length) this.copy(cnt=cnt + matchCnt, status=CalculatorStatus.OK)
      else RegexMatchMetricCalculator(
        cnt + matchCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - matchCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("REGEX_MATCH" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[RegexMatchMetricCalculator]
      RegexMatchMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }

  /**
   * Calculates amount of rows that match the provided regular expression
   *
   * Important! Any regex match is considered as a failure
   *
   * @param cnt      current counter
   * @param paramMap should contain regex
   */
  case class RegexMismatchMetricCalculator(cnt: Int,
                                           paramMap: Map[String, Any],
                                           protected val status: CalculatorStatus = CalculatorStatus.OK,
                                           protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val regex: String = paramMap("regex").toString

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val mismatchCnt: Int = values.count(elem => tryToString(elem) match {
        case Some(x) => !x.matches(regex)
        case None => true  // none is considered as a regex mismatch
      })
      if (mismatchCnt == values.length) this.copy(cnt=cnt + mismatchCnt, status=CalculatorStatus.OK)
      else RegexMismatchMetricCalculator(
        cnt + mismatchCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - mismatchCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("REGEX_MISMATCH" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[RegexMismatchMetricCalculator]
      RegexMismatchMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }

  /**
   * Calculates amount of null values in processed elements
   *
   * Important! This metric has a reversed status behaviour: it fails when nulls are found.
   *
   * @param cnt Current amount of null values
   * @return result map with keys:
   *         "NULL_VALUES"
   */
  case class NullValuesMetricCalculator(cnt: Int,
                                        protected val status: CalculatorStatus = CalculatorStatus.OK,
                                        protected val failCount: Int = 0)
    extends StatusableCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val null_cnt = values.count(_ == null) // count nulls over all columns provided
      if (null_cnt > 0) {
        NullValuesMetricCalculator(
          cnt + null_cnt,
          CalculatorStatus.FAILED,
          failCount + null_cnt)
      } else this.copy(status=CalculatorStatus.OK)
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("NULL_VALUES" -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2Casted = m2.asInstanceOf[NullValuesMetricCalculator]
      NullValuesMetricCalculator(
        this.cnt + m2Casted.cnt,
        this.status,
        this.failCount + m2Casted.getFailCounter
      )
    }
  }

  /**
   * Calculates completeness of values in the specified column
   *
   * @param nullCnt  Current amount of null values
   * @param cellCnt  Current amount of cells
   * @param paramMap Required configuration map. May contains:
   *                 optional "includeEmptyStrings" - flag which sets whether empty strings are considered in addition to null values.
   *                 (if omitted then empty strings are not considered)
   * @return result map with keys:
   *         "COMPLETENESS"
   */
  case class CompletenessMetricCalculator(nullCnt: Int, cellCnt: Int, paramMap: ParamMap) extends MetricCalculator {

    private val incEmptyStrings: Boolean = Try(paramMap("includeEmptyStrings").toString.toBoolean).getOrElse(false)

    def this(paramMap: Map[String, Any]) {
      this(0, 0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowNullCnt = values.count {
        case null => true
        case v: String if v == "" && incEmptyStrings => true
        case _ => false
      }
      CompletenessMetricCalculator(nullCnt + rowNullCnt, cellCnt + values.length, paramMap)
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("COMPLETENESS" -> ((cellCnt - nullCnt).toDouble / cellCnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = CompletenessMetricCalculator(
      this.nullCnt + m2.asInstanceOf[CompletenessMetricCalculator].nullCnt,
      this.cellCnt + m2.asInstanceOf[CompletenessMetricCalculator].cellCnt,
      paramMap
    )
  }

  /**
   * Calculates amount of empty strings in processed elements
   *
   * Important! This metric has a reversed status behaviour: it fails when empty strings are found.
   *
   * @param cnt Current amount of empty strings
   * @return result map with keys:
   *         "EMPTY_VALUES"
   */
  case class EmptyStringValuesMetricCalculator(cnt: Int,
                                               protected val status: CalculatorStatus = CalculatorStatus.OK,
                                               protected val failCount: Int = 0)
    extends StatusableCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowEmptyStrCount = values.count {
        case v: String if v == "" => true
        case _ => false
      }
      if (rowEmptyStrCount > 0) {
        EmptyStringValuesMetricCalculator(
          cnt + rowEmptyStrCount,
          CalculatorStatus.FAILED,
          failCount + rowEmptyStrCount
        )
      } else this.copy(status=CalculatorStatus.OK)
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("EMPTY_VALUES" -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[EmptyStringValuesMetricCalculator]
      EmptyStringValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

  }

  /**
   * Calculates minimal length of processed elements
   *
   * @param strl Current minimal string length
   * @return result map with keys:
   *         "MIN_STRING"
   */
  case class MinStringValueMetricCalculator(strl: Int)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(Int.MaxValue)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      // .min will throw "UnsupportedOperationException" when applied to empty sequence.
      val currentMin = Try(values.flatMap(tryToString).map(_.length).min).toOption
      currentMin match {
        case Some(v) => MinStringValueMetricCalculator(Math.min(v, strl))
        case None => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("MIN_STRING" -> (strl.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      MinStringValueMetricCalculator(
        Math.min(this.strl,
          m2.asInstanceOf[MinStringValueMetricCalculator].strl))

  }

  /**
   * Calculates maximal length of processed elements
   *
   * @param strl Current maximal string length
   * @return result map with keys:
   *         "MAX_STRING"
   */
  case class MaxStringValueMetricCalculator(strl: Double)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(Int.MinValue)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      // .max will throw "UnsupportedOperationException" when applied to empty sequence.
      val currentMax = Try(values.flatMap(tryToString).map(_.length).max).toOption
      currentMax match {
        case Some(v) => MaxStringValueMetricCalculator(Math.max(v, strl))
        case None => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("MAX_STRING" -> (strl, None))

    override def merge(m2: MetricCalculator): MetricCalculator =
      MaxStringValueMetricCalculator(
        Math.max(this.strl,
          m2.asInstanceOf[MaxStringValueMetricCalculator].strl))

  }

  /**
   * Calculates average length of processed elements
   *
   * @param sum Current sum of lengths
   * @param cnt Current count of elements
   * @return result map with keys:
   *         "AVG_STRING"
   */
  case class AvgStringValueMetricCalculator(sum: Double, cnt: Int)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) {
      this(0, 0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val sumWithCnt = values.flatMap(tryToString).map(_.length)
        .foldLeft((0.0, 0))((acc, v) => (acc._1 + v, acc._2 + 1))

      sumWithCnt match {
        case (0.0, 0) => this // empty list
        case v => AvgStringValueMetricCalculator(sum + v._1, cnt + v._2)
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("AVG_STRING" -> (sum / cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val cm2 = m2.asInstanceOf[AvgStringValueMetricCalculator]
      AvgStringValueMetricCalculator(
        this.sum + cm2.sum,
        this.cnt + cm2.cnt
      )
    }

  }

  /**
   * Calculates amount of strings in provided date format
   *
   * @param cnt      Current count of filtered elements
   * @param paramMap Required configuration map. May contains:
   *                 required "dateFormat" - requested date format
   * @return result map with keys:
   *         "FORMATTED_DATE"
   */
  case class DateFormattedValuesMetricCalculator(cnt: Double,
                                                 paramMap: ParamMap,
                                                 protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                 protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val dateFormat: String = Try(paramMap("dateFormat").toString).toOption.getOrElse("yyyy-MM-dd'T'HH:mm:ss.SSSZ")

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val formatMatchCnt = values.count(x => checkDate(x, dateFormat))
      if (formatMatchCnt == values.length) this.copy(cnt=cnt + formatMatchCnt, status=CalculatorStatus.OK)
      else DateFormattedValuesMetricCalculator(
        cnt + formatMatchCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - formatMatchCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map("FORMATTED_DATE" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[DateFormattedValuesMetricCalculator]
      DateFormattedValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

    private def checkDate(value: Any, dateFormat: String): Boolean = tryToDate(value, dateFormat).size == 1
  }

  /**
   * Calculates amount of strings with specific requested length
   *
   * @param cnt      Current count of filtered elements
   * @param paramMap Required configuration map. May contains:
   *                 required "length" - requested length
   *                 required "compareRule" - comparison rule. Could be:
   *                 "eq" - equals to,
   *                 "lt" - less than,
   *                 "lte" - less than or equals to,
   *                 "gt" - greater than,
   *                 "gte" - greater than or equals to.
   * @return result map with keys:
   *         "STRING_LENGTH"
   */
  case class StringLengthValuesMetricCalculator(cnt: Double,
                                                paramMap: ParamMap,
                                                protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val length: Int = paramMap("length").toString.toInt
    private val compareRule: String = paramMap("compareRule").toString

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).map(_.length).count(x => compareFunc(compareRule)(x, length))
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else StringLengthValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "STRING_LENGTH" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[StringLengthValuesMetricCalculator]
      StringLengthValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

    private def compareFunc(compareRule: String): (Int, Int) => Boolean = compareRule match {
      case "eq" => (value, threshold) => value == threshold
      case "lt" => (value, threshold) => value < threshold
      case "lte" => (value, threshold) => value <= threshold
      case "gt" => (value, threshold) => value > threshold
      case "gte" => (value, threshold) => value >= threshold
    }

  }

  /**
   * Caclulates amount of strings from provided domain
   *
   * @param cnt      Current count of filtered elements
   * @param paramMap Required configuration map. May contains:
   *                 required "domainSet" - set of strings that represents the requested domain
   * @return result map with keys:
   *         "STRING_IN_DOMAIN"
   */
  case class StringInDomainValuesMetricCalculator(cnt: Double,
                                                  paramMap: ParamMap,
                                                  protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                  protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val domain = paramMap("domain").asInstanceOf[List[String]].toSet

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(domain.contains)
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else StringInDomainValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "STRING_IN_DOMAIN" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[StringInDomainValuesMetricCalculator]
      StringInDomainValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }

  /**
   * Calculates amount of strings out of provided domain
   *
   * @param cnt      Current count of filtered elements
   * @param paramMap Required configuration map. May contains:
   *                 required "domainSet" - set of strings that represents the requested domain
   * @return result map with keys:
   *         "STRING_OUT_DOMAIN"
   */
  case class StringOutDomainValuesMetricCalculator(cnt: Double,
                                                   paramMap: ParamMap,
                                                   protected val status: CalculatorStatus = CalculatorStatus.OK,
                                                   protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val domain = paramMap("domain").asInstanceOf[List[String]].toSet

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(x => !domain.contains(x))
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else StringOutDomainValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "STRING_OUT_DOMAIN" + getParametrizedMetricTail(paramMap) -> (cnt, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[StringOutDomainValuesMetricCalculator]
      StringOutDomainValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }

  }

  /**
   * Counts number of appearances of requested string in processed elements
   *
   * @param cnt      Current amount of appearances
   * @param paramMap Required configuration map. May contains:
   *                 required "compareValue" - requested string to find
   * @return result map with keys:
   *         "STRING_VALUES"
   */
  case class StringValuesMetricCalculator(cnt: Int,
                                          paramMap: ParamMap,
                                          protected val status: CalculatorStatus = CalculatorStatus.OK,
                                          protected val failCount: Int = 0)
    extends StatusableCalculator {

    private val lvalue: String = paramMap("compareValue").toString

    def this(paramMap: Map[String, Any]) {
      this(0, paramMap)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(_ == lvalue)
      if (rowCnt == values.length) this.copy(cnt=cnt + rowCnt, status=CalculatorStatus.OK)
      else StringValuesMetricCalculator(
        cnt + rowCnt,
        paramMap,
        CalculatorStatus.FAILED,
        failCount + values.length - rowCnt
      )
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "STRING_VALUES" + getParametrizedMetricTail(paramMap) -> (cnt.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val m2casted = m2.asInstanceOf[StringValuesMetricCalculator]
      StringValuesMetricCalculator(
        this.cnt + m2casted.cnt,
        paramMap,
        this.status,
        this.failCount + m2casted.getFailCounter
      )
    }
  }
}
