package ru.raiffeisen.checkita.core.metrics.regular

import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Helpers.{tryToDate, tryToString}
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName}

import scala.util.Try


/**
 * Basic metrics that can be applied to string (or string like) elements
 */
object BasicStringMetrics {

  /**
   * Calculates count of distinct values in processed elements
   * WARNING: Uses set without any kind of trimming and hashing. Returns the exact count.
   * So if a big diversion of elements needs to be processed and exact result is not mandatory,
   * then it's better to use HyperLogLog version called "APPROXIMATE_DISTINCT_VALUES".
   * @param uniqueValues Set of processed values
   * @return result map with keys: "DISTINCT_VALUES"
   */
  case class DistinctValuesMetricCalculator(uniqueValues: Set[Any] = Set.empty[Any],
                                            protected val failCount: Long = 0,
                                            protected val status: CalculatorStatus = CalculatorStatus.Success,
                                            protected val failMsg: String = "OK") extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Set.empty[Any])
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator =
      DistinctValuesMetricCalculator(uniqueValues ++ values.flatMap(tryToString).toSet, failCount)

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.DistinctValues.entryName -> (uniqueValues.size.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[DistinctValuesMetricCalculator]
      DistinctValuesMetricCalculator(
        this.uniqueValues ++ that.uniqueValues,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Calculates amount of values that match the provided regular expression
   *
   * @param cnt Current counter
   * @param regex Regex pattern
   * @return result map with keys: "REGEX_MATCH"
   */
  case class RegexMatchMetricCalculator(cnt: Long,
                                        regex: String,
                                        protected val failCount: Long = 0,
                                        protected val status: CalculatorStatus = CalculatorStatus.Success,
                                        protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(regex: String) = this(0, regex)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val matchCnt: Int = values.count(elem => tryToString(elem) match {
        case Some(x) => x.matches(regex)
        case None => false
      })
      if (matchCnt == values.length) 
        RegexMatchMetricCalculator(cnt + matchCnt, regex, failCount)
      else RegexMatchMetricCalculator(
        cnt + matchCnt,
        regex,
        failCount + values.length - matchCnt,
        CalculatorStatus.Failure,
        "Some of the values failed to match regex pattern."
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RegexMatch.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[RegexMatchMetricCalculator]
      RegexMatchMetricCalculator(
        this.cnt + that.cnt,
        regex,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of rows that do not match the provided regular expression
   *
   * Important! Any regex match is considered as a failure
   *
   * @param cnt Current counter
   * @param regex Regex pattern
   * @return result map with keys: "REGEX_MISMATCH"
   */
  case class RegexMismatchMetricCalculator(cnt: Long,
                                           regex: String,
                                           protected val failCount: Long = 0,
                                           protected val status: CalculatorStatus = CalculatorStatus.Success,
                                           protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(regex: String) = this(0, regex)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val mismatchCnt: Int = values.count(elem => tryToString(elem) match {
        case Some(x) => !x.matches(regex)
        case None => true
      })
      if (mismatchCnt == values.length)
        RegexMismatchMetricCalculator(cnt + mismatchCnt, regex, failCount)
      else RegexMismatchMetricCalculator(
        cnt + mismatchCnt,
        regex,
        failCount + values.length - mismatchCnt,
        CalculatorStatus.Failure,
        "Some of the values failed to mismatch regex pattern."
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RegexMismatch.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[RegexMismatchMetricCalculator]
      RegexMismatchMetricCalculator(
        this.cnt + that.cnt,
        regex,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of null values in processed elements
   *
   * Important! This metric has a reversed status behaviour: it fails when nulls are found.
   *
   * @param cnt Current amount of null values
   * @return result map with keys: "NULL_VALUES"
   */
  case class NullValuesMetricCalculator(cnt: Long,
                                        protected val failCount: Long = 0,
                                        protected val status: CalculatorStatus = CalculatorStatus.Success,
                                        protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val null_cnt = values.count(_ == null) // count nulls over all columns provided
      if (null_cnt > 0) {
        NullValuesMetricCalculator(
          cnt + null_cnt,
          failCount + null_cnt,
          CalculatorStatus.Failure,
          s"There are $null_cnt nulls are found within processed values."
        )
      } else NullValuesMetricCalculator(cnt, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NullValues.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NullValuesMetricCalculator]
      NullValuesMetricCalculator(
        this.cnt + that.cnt,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates completeness of values in the specified column
   *
   * @param nullCnt  Current amount of null values
   * @param cellCnt  Current amount of cells
   * @param includeEmptyStrings Flag which sets whether empty strings are considered in addition to null values.
   * @return result map with keys: "COMPLETENESS"
   */
  case class CompletenessMetricCalculator(nullCnt: Long,
                                          cellCnt: Long,
                                          includeEmptyStrings: Boolean,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK") extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(includeEmptyStrings: Boolean) = this(0, 0, includeEmptyStrings)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowNullCnt = values.count {
        case null => true
        case v: String if v == "" && includeEmptyStrings => true
        case _ => false
      }
      CompletenessMetricCalculator(nullCnt + rowNullCnt, cellCnt + values.length, includeEmptyStrings, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.Completeness.entryName -> ((cellCnt - nullCnt).toDouble / cellCnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[CompletenessMetricCalculator]
      CompletenessMetricCalculator(
        this.nullCnt + that.nullCnt,
        this.cellCnt + that.cellCnt,
        this.includeEmptyStrings,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of empty strings in processed elements
   *
   * Important! This metric has a reversed status behaviour: it fails when empty strings are found.
   *
   * @param cnt Current amount of empty strings
   * @return result map with keys: "EMPTY_VALUES"
   */
  case class EmptyStringValuesMetricCalculator(cnt: Long,
                                               protected val failCount: Long = 0,
                                               protected val status: CalculatorStatus = CalculatorStatus.Success,
                                               protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowEmptyStrCount = values.count {
        case v: String if v == "" => true
        case _ => false
      }
      if (rowEmptyStrCount > 0) {
        EmptyStringValuesMetricCalculator(
          cnt + rowEmptyStrCount,
          failCount + rowEmptyStrCount,
          CalculatorStatus.Failure,
          s"There are $rowEmptyStrCount empty strings are found within processed values."
        )
      } else EmptyStringValuesMetricCalculator(cnt, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.EmptyValues.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[EmptyStringValuesMetricCalculator]
      EmptyStringValuesMetricCalculator(
        this.cnt + that.cnt,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates minimal length of processed elements
   *
   * @param strl Current minimal string length
   * @return result map with keys:  "MIN_STRING"
   */
  case class MinStringValueMetricCalculator(strl: Int,
                                            protected val failCount: Long = 0,
                                            protected val status: CalculatorStatus = CalculatorStatus.Success,
                                            protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Int.MaxValue)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      // .min will throw "UnsupportedOperationException" when applied to empty sequence.
      val currentMin = Try(values.flatMap(tryToString).map(_.length).min).toOption
      currentMin match {
        case Some(v) => MinStringValueMetricCalculator(Math.min(v, strl), failCount)
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Couldn't calculate minimum string length out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MinString.entryName -> (strl.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[MinStringValueMetricCalculator]
      MinStringValueMetricCalculator(
        Math.min(this.strl, that.strl),
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Calculates maximal length of processed elements
   *
   * @param strl Current maximal string length
   * @return result map with keys: "MAX_STRING"
   */
  case class MaxStringValueMetricCalculator(strl: Int,
                                            protected val failCount: Long = 0,
                                            protected val status: CalculatorStatus = CalculatorStatus.Success,
                                            protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Int.MinValue)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      // .max will throw "UnsupportedOperationException" when applied to empty sequence.
      val currentMax = Try(values.flatMap(tryToString).map(_.length).max).toOption
      currentMax match {
        case Some(v) => MaxStringValueMetricCalculator(Math.max(v, strl), failCount)
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Couldn't calculate maximum string length out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MaxString.entryName -> (strl.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[MaxStringValueMetricCalculator]
      MaxStringValueMetricCalculator(
        Math.max(this.strl, that.strl),
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Calculates average length of processed elements
   *
   * @param sum Current sum of lengths
   * @param cnt Current count of elements
   * @return result map with keys: "AVG_STRING"
   */
  case class AvgStringValueMetricCalculator(sum: Double,
                                            cnt: Long,
                                            protected val failCount: Long = 0,
                                            protected val status: CalculatorStatus = CalculatorStatus.Success,
                                            protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, 0)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val sumWithCnt = values.flatMap(tryToString).map(_.length)
        .foldLeft((0.0, 0))((acc, v) => (acc._1 + v, acc._2 + 1))

      sumWithCnt match {
        case (0.0, 0) => copyWithError(
          CalculatorStatus.Failure,
          "Couldn't calculate average string length for provided values."
        )
        case v => AvgStringValueMetricCalculator(sum + v._1, cnt + v._2, failCount)
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.AvgString.entryName -> (sum / cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[AvgStringValueMetricCalculator]
      AvgStringValueMetricCalculator(
        this.sum + that.sum,
        this.cnt + that.cnt,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Calculates amount of strings in provided date format
   *
   * @param cnt Current count of filtered elements
   * @param dateFormat Requested date format
   * @return result map with keys:
   *         "FORMATTED_DATE"
   */
  case class DateFormattedValuesMetricCalculator(cnt: Long,
                                                 dateFormat: String,
                                                 protected val failCount: Long = 0,
                                                 protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                 protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(dateFormat: String) = this(0, dateFormat)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val formatMatchCnt = values.count(checkDate)
      if (formatMatchCnt == values.length) DateFormattedValuesMetricCalculator(
        cnt + formatMatchCnt, dateFormat, failCount
      )
      else DateFormattedValuesMetricCalculator(
        cnt + formatMatchCnt,
        dateFormat,
        failCount + values.length - formatMatchCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values cannot be casted to date with given format of '$dateFormat'."
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.FormattedDate.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[DateFormattedValuesMetricCalculator]
      DateFormattedValuesMetricCalculator(
        this.cnt + that.cnt,
        this.dateFormat,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def checkDate(value: Any): Boolean = tryToDate(value, dateFormat).nonEmpty
  }

  /**
   * Calculates amount of strings with specific requested length
   *
   * @param cnt Current count of filtered elements
   * @param length Requested length
   * @param compareRule Comparison rule. Could be:
   *                    - "eq" - equals to,
   *                    - "lt" - less than,
   *                    - "lte" - less than or equals to,
   *                    - "gt" - greater than,
   *                    - "gte" - greater than or equals to.
   * @return result map with keys: "STRING_LENGTH"
   */
  case class StringLengthValuesMetricCalculator(cnt: Long,
                                                length: Int,
                                                compareRule: String,
                                                protected val failCount: Long = 0,
                                                protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(length: Int, compareRule: String) = this(0, length, compareRule)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).map(_.length).count(compareFunc)
      if (rowCnt == values.length) StringLengthValuesMetricCalculator(
        cnt + rowCnt, length, compareRule, failCount
      )
      else StringLengthValuesMetricCalculator(
        cnt + rowCnt,
        length,
        compareRule,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values do not meet string length criteria '$criteriaStringRepr'"
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringLength.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[StringLengthValuesMetricCalculator]
      StringLengthValuesMetricCalculator(
        this.cnt + that.cnt,
        this.length,
        this.compareRule,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def compareFunc(value: Int): Boolean = compareRule match {
      case "eq" => value == length
      case "lt" => value < length
      case "lte" => value <= length
      case "gt" => value > length
      case "gte" => value >= length
    }
    
    private def criteriaStringRepr: String = compareRule match {
      case "eq" => s"==$length"
      case "lt" => s"<$length"
      case "lte" => s"<=$length"
      case "gt" => s">$length"
      case "gte" => s">=$length"
    }
  }

  /**
   * Calculates amount of strings from provided domain
   *
   * @param cnt Current count of filtered elements
   * @param domain Set of strings that represents the requested domain
   * @return result map with keys: "STRING_IN_DOMAIN"
   */
  case class StringInDomainValuesMetricCalculator(cnt: Long,
                                                  domain: Set[String],
                                                  protected val failCount: Long = 0,
                                                  protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                  protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(domain: Set[String]) = this(0, domain)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(domain.contains)
      if (rowCnt == values.length) StringInDomainValuesMetricCalculator(
        cnt=cnt + rowCnt, domain, failCount
      )
      else StringInDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}"
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringInDomain.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[StringInDomainValuesMetricCalculator]
      StringInDomainValuesMetricCalculator(
        this.cnt + that.cnt,
        this.domain,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of strings out of provided domain
   *
   * @param cnt Current count of filtered elements
   * @param domain Set of strings that represents the requested domain
   * @return result map with keys: "STRING_OUT_DOMAIN"
   */
  case class StringOutDomainValuesMetricCalculator(cnt: Long,
                                                   domain: Set[String],
                                                   protected val failCount: Long = 0,
                                                   protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                   protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(domain: Set[String]) = this(0, domain)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(x => !domain.contains(x))
      if (rowCnt == values.length) StringOutDomainValuesMetricCalculator(
        cnt=cnt + rowCnt, domain, failCount
      )
      else StringOutDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values are IN the given domain of ${domain.mkString("[", ",", "]")}"
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringOutDomain.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[StringOutDomainValuesMetricCalculator]
      StringOutDomainValuesMetricCalculator(
        this.cnt + that.cnt,
        domain,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

  }

  /**
   * Counts number of appearances of requested string in processed elements
   *
   * @param cnt Current amount of appearances
   * @param compareValue Requested string to find
   * @return result map with keys: "STRING_VALUES"
   */
  case class StringValuesMetricCalculator(cnt: Long,
                                          compareValue: String,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this(compareValue: String) = this(0, compareValue)
    
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(_ == compareValue)
      if (rowCnt == values.length) StringValuesMetricCalculator(
        cnt=cnt + rowCnt, compareValue, failCount
      )
      else StringValuesMetricCalculator(
        cnt + rowCnt,
        compareValue,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values are not equal to requested string value of '$compareValue'"
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringValues.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[StringValuesMetricCalculator]
      StringValuesMetricCalculator(
        this.cnt + that.cnt,
        compareValue,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }
}
