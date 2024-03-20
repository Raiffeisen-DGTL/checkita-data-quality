package ru.raiffeisen.checkita.core.metrics.regular

import com.twitter.algebird.HyperLogLog.long2Bytes
import com.twitter.algebird.{HLL, HyperLogLog, HyperLogLogMonoid, SpaceSaver}
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.Casting.{tryToLong, tryToString}
import ru.raiffeisen.checkita.core.metrics.{MetricCalculator, MetricName}

/**
 * Metrics based on using the Algebird library (abstract algebra for Scala)
 * https://github.com/twitter/algebird
 */
object AlgebirdMetrics {

  /**
   * Calculates number of distinct values in processed elements
   *
   * Works for single column only!
   *
   * @param hLL Initial HyperLogLog monoid
   * @param bitsNumber Size of HLL (calculates automatically for a specific accuracy error)
   * @param accuracyError Error of calculation.
   *
   * @return result map with keys: "APPROXIMATE_DISTINCT_VALUES"
   */
  case class HyperLogLogMetricCalculator(hLL: HLL,
                                         bitsNumber: Int,
                                         accuracyError: Double,
                                         protected val failCount: Long = 0,
                                         protected val status: CalculatorStatus = CalculatorStatus.Success,
                                         protected val failMsg: String = "OK")
    extends MetricCalculator {

    // Auxiliary constrictor to init metric calculator:
    // Accuracy is limited to a value that correspond to bits number of 30 or less.
    // This is done to prevent integer overflow when computing HLL monoid size.
    def this(accuracyError: Double) = this(
      new HyperLogLogMonoid(HyperLogLog.bitsForError(math.max(accuracyError, 0.00003174))).zero,
      HyperLogLog.bitsForError(math.max(accuracyError, 0.00003174)),
      accuracyError
    )

    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "approximateDistinctValues metric works for single column only!")
      tryToString(values.head) match {
        case Some(v) =>
          val monoid = new HyperLogLogMonoid(this.bitsNumber)
          val valToAdd = if (v.trim == "") "EMPTY_VAL" else v

          HyperLogLogMetricCalculator(
            monoid.plus(hLL, monoid.create(valToAdd.getBytes())),
            bitsNumber,
            accuracyError,
            failCount
          )
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Provided value cannot be casted to string"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator = 
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] =
      Map(MetricName.ApproximateDistinctValues.entryName -> (hLL.approximateSize.estimate.toDouble, None))

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[HyperLogLogMetricCalculator]
      HyperLogLogMetricCalculator(
        this.hLL + that.hLL,
        this.bitsNumber,
        this.accuracyError,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates approximate completeness of incremental integer (long) sequence,
   * i.e. checks if sequence does not have missing elements.
   * Check is performed using variance algorithm HyperLogLog.
   * Works for single column only!
   *
   * @param hLL Initial HyperLogLog monoid
   * @param bitsNumber Size of HLL (calculates automatically for a specific accuracy error)
   * @param minVal Minimum observed value in a sequence
   * @param maxVal Maximum observed value in a sequence
   * @param accuracyError Error of calculation
   * @param increment Sequence increment
   * @return result map with keys: "APPROXIMATE_SEQUENCE_COMPLETENESS"
   */
  case class HLLSequenceCompletenessMetricCalculator(hLL: HLL,
                                                     bitsNumber: Int,
                                                     minVal: Long,
                                                     maxVal: Long,
                                                     accuracyError: Double,
                                                     increment: Long,
                                                     protected val failCount: Long = 0,
                                                     protected val status: CalculatorStatus = CalculatorStatus.Success,
                                                     protected val failMsg: String = "OK")
    extends MetricCalculator {
    
    // axillary constructor to initiate HyperLogLog monoid:
    def this(accuracyError: Double, increment: Long) = this(
      new HyperLogLogMonoid(HyperLogLog.bitsForError(accuracyError)).zero,
      HyperLogLog.bitsForError(accuracyError),
      Long.MaxValue,
      Long.MinValue,
      accuracyError,
      increment
    )

    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "approximateSequenceCompleteness metric works with single column only!")
      tryToLong(values.head) match {
        case Some(value) =>
          val monoid = new HyperLogLogMonoid(this.bitsNumber)

          HLLSequenceCompletenessMetricCalculator(
            monoid.plus(hLL, monoid.create(value)),
            this.bitsNumber,
            Math.min(minVal, value),
            Math.max(maxVal, value),
            accuracyError,
            increment,
            failCount
          )
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Provided value cannot be casted to string"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Option[String])] = Map(
        MetricName.ApproximateSequenceCompleteness.entryName ->
        (hLL.approximateSize.estimate.toDouble / ((maxVal - minVal).toDouble / increment.toDouble + 1), None)
    )

    def merge(m2: MetricCalculator): MetricCalculator = {
      val monoid = new HyperLogLogMonoid(this.bitsNumber)
      val that = m2.asInstanceOf[HLLSequenceCompletenessMetricCalculator]
      HLLSequenceCompletenessMetricCalculator(
        monoid.plus(this.hLL, that.hLL),
        this.bitsNumber,
        Math.min(this.minVal, that.minVal),
        Math.max(this.maxVal, that.maxVal),
        this.accuracyError,
        this.increment,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

  /**
   * Calculates top N elements out of processed elements
   *
   * Works for single column only!
   *
   * @param list Initial SpaceSaver monoid
   * @param maxCapacity Maximal size of SpaceSaver
   * @param targetNumber Required N
   *
   * @return result map with keys: "TOP_N_{index}"
   */
  case class TopKMetricCalculator(list: SpaceSaver[String],
                                  maxCapacity: Int,
                                  targetNumber: Int,
                                  rowCount: Int,
                                  protected val failCount: Long = 0,
                                  protected val status: CalculatorStatus = CalculatorStatus.Success,
                                  protected val failMsg: String = "OK")
    extends MetricCalculator {
    
    // axillary constructor to initiate empty SpaceSaver:
    def this(maxCapacity: Int, targetNumber: Int) = this(
      SpaceSaver(maxCapacity, "", 0),
      maxCapacity,
      targetNumber,
      0
    )

    protected def tryToIncrement(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "topN metric works for single column only!")
      tryToString(values.head) match {
        case Some(v) =>
          val newSPaceSave = list ++ SpaceSaver(maxCapacity, v)
          TopKMetricCalculator(
            newSPaceSave, maxCapacity, targetNumber, rowCount + 1, failCount
          )
        case None => copyWithError(
          CalculatorStatus.Failure,
          "Provided value cannot be casted to string"
        )
      }
    }

    protected def copyWithError(status: CalculatorStatus, msg: String, failInc: Long = 1): MetricCalculator =
      this.copy(failCount = failCount + failInc, status = status, failMsg = msg)
    
    def result(): Map[String, (Double, Some[String])] = {
      list.topK(targetNumber).zipWithIndex.map(x =>
          (MetricName.TopN.entryName + "_" + (x._2 + 1) ,
            (x._1._2.estimate.toDouble / rowCount.toDouble, Some(x._1._1)))
      ).toMap
    }

    def merge(m2: MetricCalculator): MetricCalculator = {
      val that = m2.asInstanceOf[TopKMetricCalculator]
      TopKMetricCalculator(
        this.list ++ that.list,
        this.maxCapacity,
        this.targetNumber,
        this.rowCount + that.rowCount,
        this.failCount + that.getFailCounter,
        this.status,
        this.failMsg
      )
    }
  }

}

