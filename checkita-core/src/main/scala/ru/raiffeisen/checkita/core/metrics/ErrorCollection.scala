package ru.raiffeisen.checkita.core.metrics

import org.apache.spark.util.AccumulatorV2
import ru.raiffeisen.checkita.core.CalculatorStatus


object ErrorCollection {

  /**
   * Excerpt from dataframe with data for which metric returned failure (or error) status
   *
   * @param status  Metric status
   * @param message Metric failure (or error) message
   * @param rowData Row data for which metric yielded failure (or error):
   *                contains only excerpt from full dataframe for given columns
   */
  case class ErrorRow(status: CalculatorStatus, message: String, rowData: Seq[String])

  /**
   * Stores all failure (or errors) for a particular metric
   *
   * @param columns Columns for which data is collected: contains metric columns and source key fields
   * @param errors  Sequence of metric errors with corresponding rows data
   */
  case class MetricErrors(columns: Seq[String], errors: Seq[ErrorRow])

  /**
   * Defines metric status
   *
   * @param id      Metric ID
   * @param status  Metric status
   * @param message Metric failure (or error) message
   */
  case class MetricStatus(id: String, status: CalculatorStatus, message: String)

  /**
   * Defines metric failure (or errors) in a way they are collected during metrics processing:
   *
   * @param columnNames   Column names for which data is collected: contains metric columns and source key fields.
   * @param metricStatues Sequence of metric statuses (all metrics that conform to given sequence of columns)
   * @param rowData       Row data for which metrics yielded failures (or errors):
   *                      contains only excerpt from full dataframe for given columns
   */
  case class AccumulatedErrors(
                                columnNames: Seq[String],
                                metricStatues: Seq[MetricStatus],
                                rowData: Seq[String]
                              )


  /**
   * Custom Spark accumulator used to collect metric errors.
   * The main idea of this accumulator is to limit maximum number of elements that are collected.
   *
   * @param limit Maximum number of elements to be collected.
   *              If limit is non-positive (<= 0) then number of collected elements is not limited.
   * @tparam T Type of element
   *
   * @note Implementation of this accumulator is similar to built-in Spark CollectionAccumulator
   *       with additional logic added to limit number of collected elements.
   */
  class LimitedCollectionAccumulator[T](limit: Int) extends AccumulatorV2[T, java.util.List[T]] {
    private var _list: java.util.List[T] = _

    private def getOrCreate: java.util.List[T] = {
      _list = Option(_list).getOrElse(new java.util.ArrayList[T]())
      _list
    }

    /**
     * Returns false if this accumulator instance has any values in it.
     */
    override def isZero: Boolean = this.synchronized(getOrCreate.isEmpty)

    override def copyAndReset(): LimitedCollectionAccumulator[T] = new LimitedCollectionAccumulator[T](limit)

    override def copy(): LimitedCollectionAccumulator[T] = {
      val newAcc = new LimitedCollectionAccumulator[T](limit)
      this.synchronized {
        newAcc.getOrCreate.addAll(getOrCreate)
      }
      newAcc
    }

    override def reset(): Unit = this.synchronized {
      _list = null
    }

    override def add(v: T): Unit = this.synchronized {
      if (limit <= 0 || getOrCreate.size() < limit) getOrCreate.add(v)
      else ()
    }

    /**
     * Merge list of this accumulator with other list
     * and ensure that resultant list contains no more
     * that `limit` number of elements.
     * @param other Other list to merge with current one.
     */
    private def mergeListWithLimit(other: java.util.List[T]): Unit = {
      val maxToAdd = limit - getOrCreate.size()
      if (other.size() <= maxToAdd) getOrCreate.addAll(other)
      else getOrCreate.addAll(other.subList(0, maxToAdd))
    }

    /**
     * Merges this accumulator instance with another one and ensure that
     * maximum number of collected elements is less than or equal to `limit`.
     * @param other Other accumulator instance to be merged.
     * @note If `limit` is non-positive (<= 0) then number of elements to be collected is not limited.
     */
    override def merge(other: AccumulatorV2[T, java.util.List[T]]): Unit = other match {
      case o: LimitedCollectionAccumulator[T] => this.synchronized {
        if (limit <= 0) getOrCreate.addAll(o.value)
        else if (getOrCreate.size() < limit) mergeListWithLimit(o.value)
        else ()
      }
      case _ => throw new UnsupportedOperationException(
        s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

    override def value: java.util.List[T] = this.synchronized {
      java.util.Collections.unmodifiableList(new java.util.ArrayList[T](getOrCreate))
    }
  }
}
