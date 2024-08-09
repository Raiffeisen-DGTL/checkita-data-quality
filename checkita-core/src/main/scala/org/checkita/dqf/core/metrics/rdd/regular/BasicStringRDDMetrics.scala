package org.checkita.dqf.core.metrics.rdd.regular

import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.metrics.rdd.Casting.{seqToString, tryToString, tryToTimestamp}
import org.checkita.dqf.core.metrics.rdd.{RDDMetricCalculator, ReversibleRDDCalculator}
import org.checkita.dqf.core.metrics.MetricName

import scala.util.Try


/**
 * Basic metrics that can be applied to string (or string like) elements
 */
object BasicStringRDDMetrics {

  /**
   * Calculates count of distinct values in processed elements
   * WARNING: Uses set without any kind of trimming and hashing. Returns the exact count.
   * So if a big diversion of elements needs to be processed and exact result is not mandatory,
   * then it's better to use HyperLogLog version called "APPROXIMATE_DISTINCT_VALUES".
   * @param uniqueValues Set of processed values
   * @return result map with keys: "DISTINCT_VALUES"
   */
  case class DistinctValuesRDDMetricCalculator(uniqueValues: Set[String] = Set.empty[String],
                                               protected val failCount: Long = 0,
                                               protected val status: CalculatorStatus = CalculatorStatus.Success,
                                               protected val failMsg: String = "OK") extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Set.empty[String])

    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = 
      if (values.forall(_ == null)) copyWithError(
        CalculatorStatus.Failure,
        if (values.size == 1) "Column value is null." else "Entire tuple of columns is null."
      ) else DistinctValuesRDDMetricCalculator(uniqueValues + seqToString(values), failCount)
    
    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.DistinctValues.entryName -> (uniqueValues.size.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[DistinctValuesRDDMetricCalculator]
      DistinctValuesRDDMetricCalculator(
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
  case class DuplicateValuesRDDMetricCalculator(numDuplicates: Long,
                                                uniqueValues: Set[String],
                                                protected val failCount: Long = 0,
                                                protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, Set.empty[String])

    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = 
    if (values.forall(_ == null)) this else { // skipping rows where entire tuple of columns is null.
      val valuesString = seqToString(values)
      if (uniqueValues.contains(valuesString)) DuplicateValuesRDDMetricCalculator(
        numDuplicates + 1,
        uniqueValues,
        failCount + 1,
        CalculatorStatus.Failure,
        "Duplicate found."
      ) else DuplicateValuesRDDMetricCalculator(
        numDuplicates,
        uniqueValues + valuesString,
        failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.DuplicateValues.entryName -> (numDuplicates.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[DuplicateValuesRDDMetricCalculator]
      DuplicateValuesRDDMetricCalculator(
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
  case class RegexMatchRDDMetricCalculator(cnt: Long,
                                           regex: String,
                                           protected val reversed: Boolean,
                                           protected val failCount: Long = 0,
                                           protected val status: CalculatorStatus = CalculatorStatus.Success,
                                           protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val matchCnt: Int = matchCounter(values)
      if (matchCnt == values.length)
        RegexMatchRDDMetricCalculator(cnt + matchCnt, regex, reversed, failCount)
      else RegexMatchRDDMetricCalculator(
        cnt + matchCnt,
        regex,
        reversed,
        failCount + 1,
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
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val matchCnt: Int = matchCounter(values)
      if (matchCnt > 0) RegexMatchRDDMetricCalculator(
        cnt + matchCnt,
        regex,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the values DO match regex pattern '$regex'."
      )
      else RegexMatchRDDMetricCalculator(cnt, regex, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RegexMatch.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[RegexMatchRDDMetricCalculator]
      RegexMatchRDDMetricCalculator(
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
   * @param cnt Current counter
   * @param regex Regex pattern
   * @return result map with keys: "REGEX_MISMATCH"
   */
  case class RegexMismatchRDDMetricCalculator(cnt: Long,
                                              regex: String,
                                              protected val reversed: Boolean,
                                              protected val failCount: Long = 0,
                                              protected val status: CalculatorStatus = CalculatorStatus.Success,
                                              protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

    // axillary constructor to init metric calculator:
    def this(regex: String, reversed: Boolean) = this(0, regex, reversed)

    private val mismatchCounter: Seq[Any] => Int = v => v.count(elem => tryToString(elem) match {
      case Some(x) => !x.matches(regex)
      case None => false // null values are omitted
    })

    /**
     * Increment metric calculator. May throw an exception.
     * Direct error collection logic implies that strings values which DO match regex pattern
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val mismatchCnt: Int = mismatchCounter(values)
      if (mismatchCnt == values.length)
        RegexMismatchRDDMetricCalculator(cnt + mismatchCnt, regex, reversed, failCount)
      else RegexMismatchRDDMetricCalculator(
        cnt + mismatchCnt,
        regex,
        reversed,
        failCount + 1,
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
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val mismatchCnt: Int = mismatchCounter(values)
      if (mismatchCnt > 0) RegexMismatchRDDMetricCalculator(
        cnt + mismatchCnt,
        regex,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the values failed to match regex pattern '$regex'."
      )
      else RegexMismatchRDDMetricCalculator(cnt, regex, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.RegexMismatch.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[RegexMismatchRDDMetricCalculator]
      RegexMismatchRDDMetricCalculator(
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
  case class NullValuesRDDMetricCalculator(cnt: Long,
                                           protected val reversed: Boolean,
                                           protected val failCount: Long = 0,
                                           protected val status: CalculatorStatus = CalculatorStatus.Success,
                                           protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val null_cnt = values.count(_ == null) // count nulls over all columns provided
      if (null_cnt < values.size) {
        NullValuesRDDMetricCalculator(
          cnt + null_cnt,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          s"There are non-null values found within processed values."
        )
      } else NullValuesRDDMetricCalculator(cnt + null_cnt, reversed, failCount)
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that any null values are considered
     * as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val null_cnt = values.count(_ == null) // count nulls over all columns provided
      if (null_cnt > 0) {
        NullValuesRDDMetricCalculator(
          cnt + null_cnt,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          s"There are null values found within processed values."
        )
      } else NullValuesRDDMetricCalculator(cnt, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.NullValues.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[NullValuesRDDMetricCalculator]
      NullValuesRDDMetricCalculator(
        this.cnt + that.cnt,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates completeness of values in the specified columns
   *
   * @param nullCnt             Current amount of null values.
   * @param cellCnt             Current amount of cells.
   * @param includeEmptyStrings Flag which sets whether empty strings are considered in addition to null values.
   * @param reversed            Boolean flag indicating whether error collection logic should be direct or reversed.
   * @return result map with keys: "COMPLETENESS"
   */
  case class CompletenessRDDMetricCalculator(nullCnt: Long,
                                             cellCnt: Long,
                                             includeEmptyStrings: Boolean,
                                             protected val reversed: Boolean,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowNullCnt = nullCounter(values)
      if (rowNullCnt < values.size) {
        val failMsg = if (includeEmptyStrings)
          s"There are non-null or non-empty values found within processed values."
        else s"There are non-null values found within processed values."
        CompletenessRDDMetricCalculator(
          nullCnt + rowNullCnt,
          cellCnt + values.size,
          includeEmptyStrings,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          failMsg
        )
      } else CompletenessRDDMetricCalculator(
        nullCnt + rowNullCnt,
        cellCnt + values.size,
        includeEmptyStrings,
        reversed,
        failCount
      )
    }

    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowNullCnt = nullCounter(values)
      if (rowNullCnt > 0) {
        val failMsg = if (includeEmptyStrings)
          s"There are null or empty values found within processed values."
        else s"There are null values found within processed values."
        CompletenessRDDMetricCalculator(
          nullCnt + rowNullCnt,
          cellCnt + values.size,
          includeEmptyStrings,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          failMsg
        )
      } else CompletenessRDDMetricCalculator(
        nullCnt,
        cellCnt + values.size,
        includeEmptyStrings,
        reversed,
        failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.Completeness.entryName -> ((cellCnt - nullCnt).toDouble / cellCnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[CompletenessRDDMetricCalculator]
      CompletenessRDDMetricCalculator(
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
   * Calculates emptiness of values in the specified columns, 
   * i.e. percentage of null values or empty values (if configured to account for empty values).
   *
   * @param nullCnt             Current amount of null values.
   * @param cellCnt             Current amount of cells.
   * @param includeEmptyStrings Flag which sets whether empty strings are considered in addition to null values.
   * @param reversed            Boolean flag indicating whether error collection logic should be direct or reversed.
   * @return result map with keys: "EMPTINESS"
   */
  case class EmptinessRDDMetricCalculator(nullCnt: Long,
                                          cellCnt: Long,
                                          includeEmptyStrings: Boolean,
                                          protected val reversed: Boolean,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowNullCnt = nullCounter(values)
      if (rowNullCnt < values.size) {
        val failMsg = if (includeEmptyStrings)
          s"There are non-null or non-empty values found within processed values."
        else s"There are non-null values found within processed values."
        EmptinessRDDMetricCalculator(
          nullCnt + rowNullCnt,
          cellCnt + values.size,
          includeEmptyStrings,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          failMsg
        )
      } else EmptinessRDDMetricCalculator(
        nullCnt + rowNullCnt,
        cellCnt + values.size,
        includeEmptyStrings,
        reversed,
        failCount
      )
    }

    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowNullCnt = nullCounter(values)
      if (rowNullCnt > 0) {
        val failMsg = if (includeEmptyStrings)
          s"There are null or empty values found within processed values."
        else s"There are null values found within processed values."
        EmptinessRDDMetricCalculator(
          nullCnt + rowNullCnt,
          cellCnt + values.size,
          includeEmptyStrings,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          failMsg
        )
      } else EmptinessRDDMetricCalculator(
        nullCnt,
        cellCnt + values.size,
        includeEmptyStrings,
        reversed,
        failCount
      )
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.Emptiness.entryName -> (nullCnt.toDouble / cellCnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[EmptinessRDDMetricCalculator]
      EmptinessRDDMetricCalculator(
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
  case class EmptyValuesRDDMetricCalculator(cnt: Long,
                                            protected val reversed: Boolean,
                                            protected val failCount: Long = 0,
                                            protected val status: CalculatorStatus = CalculatorStatus.Success,
                                            protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowEmptyStrCount = values.count {
        case v: String if v == "" => true
        case _ => false
      }
      if (rowEmptyStrCount < values.size) {
        EmptyValuesRDDMetricCalculator(
          cnt + rowEmptyStrCount,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          s"There are non-empty strings found within processed values."
        )
      } else EmptyValuesRDDMetricCalculator(cnt + rowEmptyStrCount, reversed, failCount)
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that any empty string values are considered
     * as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowEmptyStrCount = values.count {
        case v: String if v == "" => true
        case _ => false
      }
      if (rowEmptyStrCount > 0) {
        EmptyValuesRDDMetricCalculator(
          cnt + rowEmptyStrCount,
          reversed,
          failCount + 1,
          CalculatorStatus.Failure,
          s"There are empty strings found within processed values."
        )
      } else EmptyValuesRDDMetricCalculator(cnt, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.EmptyValues.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[EmptyValuesRDDMetricCalculator]
      EmptyValuesRDDMetricCalculator(
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
  case class MinStringRDDMetricCalculator(strl: Int,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Int.MaxValue)

    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      // .min will throw "UnsupportedOperationException" when applied to empty sequence.
      val currentMin = Try(values.flatMap(tryToString).map(_.length).min).toOption
      currentMin match {
        case Some(v) => MinStringRDDMetricCalculator(Math.min(v, strl), failCount)
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Failed to calculate minimum string length out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MinString.entryName -> (strl.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[MinStringRDDMetricCalculator]
      MinStringRDDMetricCalculator(
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
  case class MaxStringRDDMetricCalculator(strl: Int,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(Int.MinValue)

    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      // .max will throw "UnsupportedOperationException" when applied to empty sequence.
      val currentMax = Try(values.flatMap(tryToString).map(_.length).max).toOption
      currentMax match {
        case Some(v) => MaxStringRDDMetricCalculator(Math.max(v, strl), failCount)
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Failed to calculate maximum string length out of provided values."
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.MaxString.entryName -> (strl.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[MaxStringRDDMetricCalculator]
      MaxStringRDDMetricCalculator(
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
   *
   * @note Null values are omitted:
   *       For values: "foo", "bar-buz", null
   *       Metric result would be: (3 + 7) / 2 = 5
   */
  case class AvgStringRDDMetricCalculator(sum: Double,
                                          cnt: Long,
                                          protected val failCount: Long = 0,
                                          protected val status: CalculatorStatus = CalculatorStatus.Success,
                                          protected val failMsg: String = "OK")
    extends RDDMetricCalculator {

    // axillary constructor to init metric calculator:
    def this() = this(0, 0)

    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val sumWithCnt = values.flatMap(tryToString).map(_.length)
        .foldLeft((0.0, 0))((acc, v) => (acc._1 + v, acc._2 + 1))

      sumWithCnt match {
        case (0.0, 0) => copyWithError(
          CalculatorStatus.Failure,
          "Failed to calculate average string length for provided values."
        )
        case v => AvgStringRDDMetricCalculator(sum + v._1, cnt + v._2, failCount)
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.AvgString.entryName -> (sum / cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[AvgStringRDDMetricCalculator]
      AvgStringRDDMetricCalculator(
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
  case class FormattedDateRDDMetricCalculator(cnt: Long,
                                              dateFormat: String,
                                              protected val reversed: Boolean,
                                              protected val failCount: Long = 0,
                                              protected val status: CalculatorStatus = CalculatorStatus.Success,
                                              protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val formatMatchCnt = values.count(checkDate)
      if (formatMatchCnt == values.length) FormattedDateRDDMetricCalculator(
        cnt + formatMatchCnt, dateFormat, reversed, failCount
      ) else FormattedDateRDDMetricCalculator(
        cnt + formatMatchCnt,
        dateFormat,
        reversed,
        failCount + 1,
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
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val formatMatchCnt = values.count(checkDate)
      if (formatMatchCnt > 0) FormattedDateRDDMetricCalculator(
        cnt + formatMatchCnt,
        dateFormat,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values CAN be cast to date with given format of '$dateFormat'."
      ) else FormattedDateRDDMetricCalculator(cnt, dateFormat, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.FormattedDate.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[FormattedDateRDDMetricCalculator]
      FormattedDateRDDMetricCalculator(
        this.cnt + that.cnt,
        this.dateFormat,
        this.reversed,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }

    private def checkDate(value: Any): Boolean = tryToTimestamp(value, dateFormat).nonEmpty
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
  case class StringLengthRDDMetricCalculator(cnt: Long,
                                             length: Int,
                                             compareRule: String,
                                             protected val reversed: Boolean,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).map(_.length).count(compareFunc)
      if (rowCnt == values.size) StringLengthRDDMetricCalculator(
        cnt + rowCnt, length, compareRule, reversed, failCount
      ) else StringLengthRDDMetricCalculator(
        cnt + rowCnt,
        length,
        compareRule,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"There are values found that do not meet string length criteria '$criteriaStringRepr'."
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
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).map(_.length).count(compareFunc)
      if (rowCnt > 0) StringLengthRDDMetricCalculator(
        cnt + rowCnt,
        length,
        compareRule,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"There are values found that DO meet string length criteria '$criteriaStringRepr'."
      )
      else StringLengthRDDMetricCalculator(cnt, length, compareRule, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringLength.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[StringLengthRDDMetricCalculator]
      StringLengthRDDMetricCalculator(
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
  case class StringInDomainRDDMetricCalculator(cnt: Long,
                                               domain: Set[String],
                                               protected val reversed: Boolean,
                                               protected val failCount: Long = 0,
                                               protected val status: CalculatorStatus = CalculatorStatus.Success,
                                               protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(domain.contains)
      if (rowCnt == values.length) StringInDomainRDDMetricCalculator(
        cnt=cnt + rowCnt, domain, reversed, failCount
      ) else StringInDomainRDDMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}."
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
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(domain.contains)
      if (rowCnt > 0) StringInDomainRDDMetricCalculator(
        cnt=cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values are IN the given domain of ${domain.mkString("[", ",", "]")}."
      )
      else StringInDomainRDDMetricCalculator(cnt, domain, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringInDomain.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[StringInDomainRDDMetricCalculator]
      StringInDomainRDDMetricCalculator(
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
  case class StringOutDomainRDDMetricCalculator(cnt: Long,
                                                domain: Set[String],
                                                protected val reversed: Boolean,
                                                protected val failCount: Long = 0,
                                                protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(x => !domain.contains(x))
      if (rowCnt == values.length) StringOutDomainRDDMetricCalculator(
        cnt=cnt + rowCnt, domain, reversed, failCount
      )
      else StringOutDomainRDDMetricCalculator(
        cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values are IN the given domain of ${domain.mkString("[", ",", "]")}."
      )
    }

    /**
     * Increment metric calculator with REVERSED error collection logic. May throw an exception.
     * Reversed error collection logic implies that strings values which are outside of provided domain
     * are considered as metric failure and are collected.
     *
     * @param values values to process
     * @return updated calculator or throws an exception
     */
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(x => !domain.contains(x))
      if (rowCnt > 0) StringOutDomainRDDMetricCalculator(
        cnt=cnt + rowCnt,
        domain,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values are not in the given domain of ${domain.mkString("[", ",", "]")}."
      ) else StringOutDomainRDDMetricCalculator(cnt, domain, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringOutDomain.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[StringOutDomainRDDMetricCalculator]
      StringOutDomainRDDMetricCalculator(
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
  case class StringValuesRDDMetricCalculator(cnt: Long,
                                             compareValue: String,
                                             protected val reversed: Boolean,
                                             protected val failCount: Long = 0,
                                             protected val status: CalculatorStatus = CalculatorStatus.Success,
                                             protected val failMsg: String = "OK")
    extends RDDMetricCalculator with ReversibleRDDCalculator {

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
    protected def tryToIncrement(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(_ == compareValue)
      if (rowCnt == values.length) StringValuesRDDMetricCalculator(
        cnt=cnt + rowCnt, compareValue, reversed, failCount
      )
      else StringValuesRDDMetricCalculator(
        cnt + rowCnt,
        compareValue,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values do not equal to requested string value of '$compareValue'."
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
    protected def tryToIncrementReversed(values: Seq[Any]): RDDMetricCalculator = {
      val rowCnt = values.flatMap(tryToString).count(_ == compareValue)
      if (rowCnt > 0) StringValuesRDDMetricCalculator(
        cnt=cnt + rowCnt,
        compareValue,
        reversed,
        failCount + 1,
        CalculatorStatus.Failure,
        s"Some of the provided values DO equal to requested string value of '$compareValue'."
      )
      else StringValuesRDDMetricCalculator(cnt, compareValue, reversed, failCount)
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): RDDMetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)

    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.StringValues.entryName -> (cnt.toDouble, None))

    def merge(m2: RDDMetricCalculator): RDDMetricCalculator = {
      val that = m2.asInstanceOf[StringValuesRDDMetricCalculator]
      StringValuesRDDMetricCalculator(
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
