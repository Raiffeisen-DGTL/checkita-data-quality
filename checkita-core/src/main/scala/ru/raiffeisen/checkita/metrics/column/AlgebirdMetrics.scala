package ru.raiffeisen.checkita.metrics.column

import com.twitter.algebird.{HLL, HyperLogLog, HyperLogLogMonoid, SpaceSaver}
import com.twitter.algebird.HyperLogLog.long2Bytes
import ru.raiffeisen.checkita.metrics.MetricCalculator
import ru.raiffeisen.checkita.metrics.MetricProcessor.ParamMap
import ru.raiffeisen.checkita.utils.{getParametrizedMetricTail, tryToLong, tryToString}

import scala.util.Try


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
   * @param paramMap Required configuration map. May contains:
   *   accuracyError - error of calculation. Default: 0.01
   *
   * @return result map with keys:
   *   "APPROXIMATE_DISTINCT_VALUES"
   */
  case class HyperLogLogMetricCalculator(hLL: HLL,
                                         bitsNumber: Int,
                                         paramMap: ParamMap)
    extends MetricCalculator {

    def this(paramMap: Map[String, Any]) = {
      this(
        new HyperLogLogMonoid(
          HyperLogLog.bitsForError(
            paramMap.getOrElse("accuracyError", 0.01d).toString.toDouble)).zero,
        HyperLogLog.bitsForError(
          paramMap.getOrElse("accuracyError", 0.01d).toString.toDouble),
        paramMap
      )
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "approximateDistinctValues metric works for single column only!")
      tryToString(values.head) match {
        case Some(v) =>
          val monoid = new HyperLogLogMonoid(this.bitsNumber)
          val valToAdd = if (v.trim == "") "EMPTY_VAL" else v

          HyperLogLogMetricCalculator(
            monoid.plus(this.hLL, monoid.create(valToAdd.getBytes())),
            this.bitsNumber,
            this.paramMap)
        case None => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "APPROXIMATE_DISTINCT_VALUES" + getParametrizedMetricTail(paramMap) -> (this.hLL.approximateSize.estimate.toDouble, None))

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val monoid = new HyperLogLogMonoid(this.bitsNumber)
      HyperLogLogMetricCalculator(
        monoid.plus(this.hLL, m2.asInstanceOf[HyperLogLogMetricCalculator].hLL),
        this.bitsNumber,
        this.paramMap)
    }
  }

  /**
   * Calculates approximate completeness of incremental integer (long) sequence,
   * i.e. checks if sequence does not have missing elements.
   * Check is performed using variance algorithm HyperLogLog.
   * Works for single column only!
   *
   * @param hLL - Initial HyperLogLog monoid
   * @param bitsNumber - Size of HLL (calculates automatically for a specific accuracy error)
   * @param minVal – minimum observed value in a sequence
   * @param maxVal – maximum observed value in a sequence
   * @param paramMap - Optional configuration map. May contains:
   *   optional key "accuracyError" - error of calculation. Default: 0.01
   *   optional key "increment" - sequence increment. Default: 1
   * @return result map with keys:
   *   "APPROXIMATE_SEQUENCE_COMPLETENESS"
   */
  case class HLLSequenceCompletenessMetricCalculator(hLL: HLL,
                                                     bitsNumber: Int,
                                                     minVal: Long,
                                                     maxVal: Long,
                                                     paramMap: ParamMap)
    extends MetricCalculator {

    private val seqInc: Long = Try(paramMap("increment").toString.toLong).getOrElse(1)

    def this(paramMap: Map[String, Any]) {
      this(
        new HyperLogLogMonoid(
          HyperLogLog.bitsForError(paramMap.getOrElse("accuracyError", 0.01d).toString.toDouble)).zero,
        HyperLogLog.bitsForError(paramMap.getOrElse("accuracyError", 0.01d).toString.toDouble),
        Long.MaxValue,
        Long.MinValue,
        paramMap
      )
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "approximateSequenceCompleteness metric works with single column only!")
      tryToLong(values.head) match {
        case Some(value) =>
          val monoid = new HyperLogLogMonoid(this.bitsNumber)

          HLLSequenceCompletenessMetricCalculator(
            monoid.plus(this.hLL, monoid.create(value)),
            this.bitsNumber,
            Math.min(minVal, value),
            Math.max(maxVal, value),
            this.paramMap
          )
        case None => this
      }
    }

    override def result(): Map[String, (Double, Option[String])] =
      Map(
        "APPROXIMATE_SEQUENCE_COMPLETENESS" + getParametrizedMetricTail(paramMap) ->
          (this.hLL.approximateSize.estimate.toDouble / ((maxVal - minVal) / seqInc + 1), None)
      )

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val monoid = new HyperLogLogMonoid(this.bitsNumber)
      HLLSequenceCompletenessMetricCalculator(
        monoid.plus(this.hLL, m2.asInstanceOf[HLLSequenceCompletenessMetricCalculator].hLL),
        this.bitsNumber,
        Math.min(this.minVal, m2.asInstanceOf[HLLSequenceCompletenessMetricCalculator].minVal),
        Math.max(this.maxVal, m2.asInstanceOf[HLLSequenceCompletenessMetricCalculator].maxVal),
        this.paramMap)
    }
  }

  /**
   * Calculates top N elemeint for processed elements
   *
   * Works for single column only!
   *
   * @param list Initial SpaceSaver monoid
   * @param paramMap Required configuration map. May contains:
   *   "maxCapacity" - maximal size of SpaceSaver. Default value: 100
   *   "targetNumber" - required N. Default value: 10
   *
   * @return result map with keys:
   *   "TOP_N_{index}"
   */
  case class TopKMetricCalculator(list: SpaceSaver[String],
                                  paramMap: ParamMap,
                                  rowcount: Int)
    extends MetricCalculator {

    private val maxCapacity: Int =
      paramMap.getOrElse("maxCapacity", 100).toString.toInt
    private val k: Int = paramMap.getOrElse("targetNumber", 10).toString.toInt

    def this(paramMap: ParamMap) = {
      this(SpaceSaver(paramMap.getOrElse("maxCapacity", 100).toString.toInt,
        "",
        0),
        paramMap,
        0)
    }

    override def increment(values: Seq[Any]): MetricCalculator = {
      assert(values.length == 1, "topN metric works for single column only!")
      tryToString(values.head) match {
        case Some(v) =>
          val newSPaceSave = this.list.++(SpaceSaver(this.maxCapacity, v))
          TopKMetricCalculator(newSPaceSave, this.paramMap, this.rowcount + 1)
        case None => this
      }
    }

    override def result(): Map[String, (Double, Some[String])] = {
      this.list
        .topK(k)
        .zipWithIndex
        .map(x =>
          ("TOP_N_" + (x._2 + 1) + getParametrizedMetricTail(paramMap),
            (x._1._2.estimate.toDouble / this.rowcount.toDouble, Some(x._1._1))))
        .toMap
    }

    override def merge(m2: MetricCalculator): MetricCalculator = {
      val addCalc = m2.asInstanceOf[TopKMetricCalculator]
      val merged = this.list ++ addCalc.list
      TopKMetricCalculator(merged,
        this.paramMap,
        this.rowcount + addCalc.rowcount)
    }
  }

}
