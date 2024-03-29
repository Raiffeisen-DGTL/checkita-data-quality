package ru.raiffeisen.checkita.core.metrics.regular

import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Casting.{seqToString, tryToDate, tryToDouble, tryToString}
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName, ReversibleCalculator}

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
      DistinctValuesMetricCalculator(uniqueValues ++ values.map(v => seqToString(Seq(v))).toSet, failCount)

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
    * Calculates number of duplicate values for given column or tuple of columns.
    * WARNING: In order to find duplicates, the processed unique values are stored as a set
    * without any kind of trimming and hashing. So if a big diversion of elements needs to be 
    * processed there is a risk of getting OOM error in cases when executors have 
    * insufficient memory allocation.
    *
    * @param numDuplicates Number of found duplicates
    * @param uniqueValues Set of unique values obtained from already processed rows
    */
  case class DuplicateValuesMetricCalculator(numDuplicates: Long,
                                             uniqueValues: Set[String],
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends MetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, Set.empty[String])

    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val valuesString = seqToString(values)
      if (uniqueValues.contains(valuesString)) DuplicateValuesMetricCalculator(
        numDuplicates + 1,
        uniqueValues,
        failCount + 1,
        CalculatorStatus.Failure,
        "Duplicate found."
      ) else DuplicateValuesMetricCalculator(
        numDuplicates,
        uniqueValues + valuesString,
        failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.DuplicateValues.entryName -> (numDuplicates.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[DuplicateValuesMetricCalculator]
      DuplicateValuesMetricCalculator(
        this.numDuplicates + that.numDuplicates + this.uniqueValues.intersect(that.uniqueValues).size,
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
                                        protected val reversed: Boolean,
                                        protected val failCount: Long = 0,
                                        protected val status: CalculatorStatus = CalculatorStatus.Success,
                                        protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(regex: String, reversed: Boolean) = this(0, regex, reversed)

    private val matchCounter: Seq[Any] => Int = v => v.count(elem => tryToString(elem) match {
      case Some(x) => x.matches(regex)
      case None => false
    })

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which failed to match regex pattern
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val matchCnt: Int = matchCounter(values)
      if (matchCnt == values.length) 
        RegexMatchMetricCalculator(cnt + matchCnt, regex, reversed, failCount)
      else RegexMatchMetricCalculator(
        cnt + matchCnt,
        regex,
        reversed,
        failCount + values.length - matchCnt,
        CalculatorStatus.Failure,
        s"Some of the values failed to match regex pattern '$regex'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which DO match regex pattern
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val matchCnt: Int = matchCounter(values)
      if (matchCnt > 0) RegexMatchMetricCalculator(
        cnt + matchCnt,
        regex,
        reversed,
        failCount + matchCnt,
        CalculatorStatus.Failure,
        s"Some of the values DO match regex pattern '$regex'."
      )
      else RegexMatchMetricCalculator(cnt, regex, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RegexMatch.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[RegexMatchMetricCalculator]
      RegexMatchMetricCalculator(
        this.cnt + that.cnt,
        this.regex,
        this.reversed,
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
                                           protected val reversed: Boolean,
                                           protected val failCount: Long = 0,
                                           protected val status: CalculatorStatus = CalculatorStatus.Success,
                                           protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(regex: String, reversed: Boolean) = this(0, regex, reversed)

    private val mismatchCounter: Seq[Any] => Int = v => v.count(elem => tryToString(elem) match {
      case Some(x) => !x.matches(regex)
      case None => true
    })

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which DO match regex pattern
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val mismatchCnt: Int = mismatchCounter(values)
      if (mismatchCnt == values.length)
        RegexMismatchMetricCalculator(cnt + mismatchCnt, regex, reversed, failCount)
      else RegexMismatchMetricCalculator(
        cnt + mismatchCnt,
        regex,
        reversed,
        failCount + values.length - mismatchCnt,
        CalculatorStatus.Failure,
        s"Some of the values DO match regex pattern '$regex'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which failed to match regex pattern
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val mismatchCnt: Int = mismatchCounter(values)
      if (mismatchCnt > 0) RegexMismatchMetricCalculator(
        cnt + mismatchCnt,
        regex,
        reversed,
        failCount + mismatchCnt,
        CalculatorStatus.Failure,
        s"Some of the values failed to match regex pattern '$regex'."
      )
      else RegexMismatchMetricCalculator(cnt, regex, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RegexMismatch.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[RegexMismatchMetricCalculator]
      RegexMismatchMetricCalculator(
        this.cnt + that.cnt,
        this.regex,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of null values in processed elements
   *
   * @param cnt      Current amount of null values
   * @param reversed Boolean flag indicating whether error collection logic should be direct or reversed.
   * @return result map with keys: "NULL_VALUES"
   */
  case class NullValuesMetricCalculator(cnt: Long,
                                        protected val reversed: Boolean,
                                        protected val failCount: Long = 0,
                                        protected val status: CalculatorStatus = CalculatorStatus.Success,
                                        protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(reversed: Boolean) = this(0, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that any non-null values are considered
     * as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val null_cnt = values.count(_ == null) // count nulls over all columns provided
      if (null_cnt < values.size) {
        NullValuesMetricCalculator(
          cnt + null_cnt,
          reversed,
          failCount + (values.size - null_cnt),
          CalculatorStatus.Failure,
          s"There are ${values.size - null_cnt} non-null values found within processed values."
        )
      } else NullValuesMetricCalculator(cnt + null_cnt, reversed, failCount)
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that any null values are considered
     * as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val null_cnt = values.count(_ == null) // count nulls over all columns provided
      if (null_cnt > 0) {
        NullValuesMetricCalculator(
          cnt + null_cnt,
          reversed,
          failCount + null_cnt,
          CalculatorStatus.Failure,
          s"There are $null_cnt null values found within processed values."
        )
      } else NullValuesMetricCalculator(cnt, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NullValues.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[NullValuesMetricCalculator]
      NullValuesMetricCalculator(
        this.cnt + that.cnt,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates completeness of values in the specified column
   *
   * @param nullCnt             Current amount of null values.
   * @param cellCnt             Current amount of cells.
   * @param includeEmptyStrings Flag which sets whether empty strings are considered in addition to null values.
   * @param reversed            Boolean flag indicating whether error collection logic should be direct or reversed.
   * @return result map with keys: "COMPLETENESS"
   */
  case class CompletenessMetricCalculator(nullCnt: Long,
                                          cellCnt: Long,
                                          includeEmptyStrings: Boolean,
                                          protected val reversed: Boolean,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(includeEmptyStrings: Boolean, reversed: Boolean) = this(0, 0, includeEmptyStrings, reversed)

    private val nullCounter: Seq[Any] => Long = v => v.count {
      case null => true
      case v: String if v == "" && includeEmptyStrings => true
      case _ => false
    }

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that any non-null
     * (or non-empty if `includeEmptyStrings` is `true`) values are considered
     * as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowNullCnt = nullCounter(values)
      if (rowNullCnt < values.size) {
        val failMsg = if (includeEmptyStrings)
          s"There are ${values.size - rowNullCnt} non-null or non-empty values found within processed values."
        else s"There are ${values.size - rowNullCnt} non-null values found within processed values."
        CompletenessMetricCalculator(
          nullCnt + rowNullCnt,
          cellCnt + values.size,
          includeEmptyStrings,
          reversed,
          failCount + (values.size - rowNullCnt),
          CalculatorStatus.Failure,
          failMsg
        )
      } else CompletenessMetricCalculator(
        nullCnt + rowNullCnt,
        cellCnt + values.size,
        includeEmptyStrings,
        reversed,
        failCount
      )
    }

    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowNullCnt = nullCounter(values)
      if (rowNullCnt > 0) {
        val failMsg = if (includeEmptyStrings)
          s"There are $rowNullCnt null or empty values found within processed values."
        else s"There are $rowNullCnt null values found within processed values."
        CompletenessMetricCalculator(
          nullCnt + rowNullCnt,
          cellCnt + values.size,
          includeEmptyStrings,
          reversed,
          failCount + rowNullCnt,
          CalculatorStatus.Failure,
          failMsg
        )
      } else CompletenessMetricCalculator(
        nullCnt,
        cellCnt + values.size,
        includeEmptyStrings,
        reversed,
        failCount
      )
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
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates amount of empty strings in processed elements.
   *
   * @param cnt      Current amount of empty strings.
   * @param reversed Boolean flag indicating whether error collection logic should be direct or reversed.
   * @return result map with keys: "EMPTY_VALUES"
   */
  case class EmptyStringValuesMetricCalculator(cnt: Long,
                                               protected val reversed: Boolean,
                                               protected val failCount: Long = 0,
                                               protected val status: CalculatorStatus = CalculatorStatus.Success,
                                               protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(reversed: Boolean) = this(0, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that any non-empty string values are considered
     * as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowEmptyStrCount = values.count {
        case v: String if v == "" => true
        case _ => false
      }
      if (rowEmptyStrCount < values.size) {
        EmptyStringValuesMetricCalculator(
          cnt + rowEmptyStrCount,
          reversed,
          failCount + (values.size - rowEmptyStrCount),
          CalculatorStatus.Failure,
          s"There are ${values.size - rowEmptyStrCount} non-empty strings found within processed values."
        )
      } else EmptyStringValuesMetricCalculator(cnt + rowEmptyStrCount, reversed, failCount)
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that any empty string values are considered
     * as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowEmptyStrCount = values.count {
        case v: String if v == "" => true
        case _ => false
      }
      if (rowEmptyStrCount > 0) {
        EmptyStringValuesMetricCalculator(
          cnt + rowEmptyStrCount,
          reversed,
          failCount + rowEmptyStrCount,
          CalculatorStatus.Failure,
          s"There are $rowEmptyStrCount empty strings found within processed values."
        )
      } else EmptyStringValuesMetricCalculator(cnt, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.EmptyValues.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[EmptyStringValuesMetricCalculator]
      EmptyStringValuesMetricCalculator(
        this.cnt + that.cnt,
        reversed,
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
                                                 protected val reversed: Boolean,
                                                 protected val failCount: Long = 0,
                                                 protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                 protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(dateFormat: String, reversed: Boolean) = this(0, dateFormat, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which cannot be cast to date with given format
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val formatMatchCnt = values.count(checkDate)
      if (formatMatchCnt == values.length) DateFormattedValuesMetricCalculator(
        cnt + formatMatchCnt, dateFormat, reversed, failCount
      ) else DateFormattedValuesMetricCalculator(
        cnt + formatMatchCnt,
        dateFormat,
        reversed,
        failCount + values.length - formatMatchCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values cannot be cast to date with given format of '$dateFormat'."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which CAN be cast to date with given format
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val formatMatchCnt = values.count(checkDate)
      if (formatMatchCnt > 0) DateFormattedValuesMetricCalculator(
        cnt + formatMatchCnt,
        dateFormat,
        reversed,
        failCount + formatMatchCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values CAN be cast to date with given format of '$dateFormat'."
      ) else DateFormattedValuesMetricCalculator(cnt, dateFormat, reversed, failCount)
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
        this.reversed,
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
                                                protected val reversed: Boolean,
                                                protected val failCount: Long = 0,
                                                protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(length: Int, compareRule: String, reversed: Boolean) = this(0, length, compareRule, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which length doesn't meet provided criteria
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).map(_.length).count(compareFunc)
      if (rowCnt == values.size) StringLengthValuesMetricCalculator(
        cnt + rowCnt, length, compareRule, reversed, failCount
      ) else StringLengthValuesMetricCalculator(
        cnt + rowCnt,
        length,
        compareRule,
        reversed,
        failCount + values.size - rowCnt,
        CalculatorStatus.Failure,
        s"There are ${values.size - rowCnt} values found that do not meet string length criteria '$criteriaStringRepr'"
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which length DOES meet provided criteria
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).map(_.length).count(compareFunc)
      if (rowCnt > 0) StringLengthValuesMetricCalculator(
        cnt + rowCnt,
        length,
        compareRule,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"There are $rowCnt values found that DO meet string length criteria '$criteriaStringRepr'"
      )
      else StringLengthValuesMetricCalculator(cnt, length, compareRule, reversed, failCount)
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
        this.reversed,
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
                                                  protected val reversed: Boolean,
                                                  protected val failCount: Long = 0,
                                                  protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                  protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(domain: Set[String], reversed: Boolean) = this(0, domain, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which are outside of provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(domain.contains)
      if (rowCnt == values.length) StringInDomainValuesMetricCalculator(
        cnt=cnt + rowCnt, domain, reversed, failCount
      ) else StringInDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}"
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which are within provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(domain.contains)
      if (rowCnt > 0) StringInDomainValuesMetricCalculator(
        cnt=cnt + rowCnt,
        domain,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values are IN the given domain of ${domain.mkString("[", ",", "]")}"
      )
      else StringInDomainValuesMetricCalculator(cnt, domain, reversed, failCount)
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
        this.reversed,
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
                                                   protected val reversed: Boolean,
                                                   protected val failCount: Long = 0,
                                                   protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                   protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(domain: Set[String], reversed: Boolean) = this(0, domain, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which are within of provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(x => !domain.contains(x))
      if (rowCnt == values.length) StringOutDomainValuesMetricCalculator(
        cnt=cnt + rowCnt, domain, reversed, failCount
      )
      else StringOutDomainValuesMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values are IN the given domain of ${domain.mkString("[", ",", "]")}"
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which outside of provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(x => !domain.contains(x))
      if (rowCnt > 0) StringOutDomainValuesMetricCalculator(
        cnt=cnt + rowCnt,
        domain,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}"
      ) else StringOutDomainValuesMetricCalculator(cnt, domain, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringOutDomain.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[StringOutDomainValuesMetricCalculator]
      StringOutDomainValuesMetricCalculator(
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
   * Counts number of appearances of requested string in processed elements
   *
   * @param cnt Current amount of appearances
   * @param compareValue Requested string to find
   * @return result map with keys: "STRING_VALUES"
   */
  case class StringValuesMetricCalculator(cnt: Long,
                                          compareValue: String,
                                          protected val reversed: Boolean,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends MetricCalculator with ReversibleCalculator {

    // axillary constructor to init metric calculator:
    def this(compareValue: String, reversed: Boolean) = this(0, compareValue, reversed)

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which are not equal to provided value
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(_ == compareValue)
      if (rowCnt == values.length) StringValuesMetricCalculator(
        cnt=cnt + rowCnt, compareValue, reversed, failCount
      )
      else StringValuesMetricCalculator(
        cnt + rowCnt,
        compareValue,
        reversed,
        failCount + values.length - rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values do not equal to requested string value of '$compareValue'"
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which are equal to provided value
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): MetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(_ == compareValue)
      if (rowCnt > 0) StringValuesMetricCalculator(
        cnt=cnt + rowCnt,
        compareValue,
        reversed,
        failCount + rowCnt,
        CalculatorStatus.Failure,
        s"Some of the provided values DO equal to requested string value of '$compareValue'"
      )
      else StringValuesMetricCalculator(cnt, compareValue, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringValues.entryName -> (cnt.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[StringValuesMetricCalculator]
      StringValuesMetricCalculator(
        this.cnt + that.cnt,
        this.compareValue,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }
}
