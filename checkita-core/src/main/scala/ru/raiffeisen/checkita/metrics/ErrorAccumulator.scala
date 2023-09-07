package ru.raiffeisen.checkita.metrics

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

final case class ErrorAccumulator(out: mutable.ArrayBuffer[(String, String, String)])
  extends AccumulatorV2[(String, String, String), mutable.ArrayBuffer[(String, String, String)]] {

  override def isZero: Boolean = value.isEmpty
  override def copy(): AccumulatorV2[(String, String, String),
    mutable.ArrayBuffer[(String, String, String)]] = ErrorAccumulator(value)
  override def reset(): Unit = value.clear()
  override def add(v: (String, String, String)): Unit = ErrorAccumulator(value += v)
  override def merge(other: AccumulatorV2[(String, String, String),
    mutable.ArrayBuffer[(String, String, String)]]): Unit =
    ErrorAccumulator(this.value ++= other.value)
  override def value: mutable.ArrayBuffer[(String, String, String)] = out
}
