package ru.raiffeisen.checkita.core.metrics.serialization

import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.auto.autoRefineV
import eu.timepit.refined.types.string.NonEmptyString
import ru.raiffeisen.checkita.config.RefinedTypes._
import ru.raiffeisen.checkita.config.jobconf.MetricParams.ApproxDistinctValuesParams
import ru.raiffeisen.checkita.config.jobconf.Metrics._
import ru.raiffeisen.checkita.core.metrics.ErrorCollection.{AccumulatedErrors, MetricStatus}
import ru.raiffeisen.checkita.core.metrics.{MetricName, RegularMetric}
import ru.raiffeisen.checkita.core.metrics.serialization.API._

trait SerializersProducts extends SerDeTransformations {
  
  private def getProductSerDe[P <: Product, T](toT: P => Option[T], fromT: T => P)
                                              (implicit tSerDe: SerDe[T]): SerDe[P] =
    transform(tSerDe, fromT, (p: P) => toT(p).get)
  
  implicit def regularMetricSerDe[R <: RegularMetric](implicit mSerDe: SerDe[MetricName]): SerDe[R] = {
    val getter: MetricName => SerDe[R] = mn => {
      val serDe = mn match {
        case MetricName.RowCount => 
          getProductSerDe(RowCountMetricConfig.unapply, RowCountMetricConfig.tupled)
        case MetricName.DuplicateValues => 
          getProductSerDe(DuplicateValuesMetricConfig.unapply, DuplicateValuesMetricConfig.tupled)
        case MetricName.DistinctValues => 
          getProductSerDe(DistinctValuesMetricConfig.unapply, DistinctValuesMetricConfig.tupled)
        case MetricName.ApproximateDistinctValues =>
          getProductSerDe(ApproxDistinctValuesMetricConfig.unapply, ApproxDistinctValuesMetricConfig.tupled)
      }
      serDe.asInstanceOf[SerDe[R]]
    }
    kinded(mSerDe, (r: R) => r.metricName, getter)
  }
  
  // SerDe's for error collection classes:
  implicit val metricStatusSerDe: SerDe[MetricStatus] = 
    getProductSerDe(MetricStatus.unapply, MetricStatus.tupled)
  implicit val accumulatedErrorsSerDe: SerDe[AccumulatedErrors] = 
    getProductSerDe(AccumulatedErrors.unapply, AccumulatedErrors.tupled)
  
  // SerDe's for refined classes:
  implicit def refinedSerDe[T, P, R <: Refined[T, P]](implicit tSerDe: SerDe[T]): SerDe[Refined[T, P]] = 
    transform(tSerDe, Refined.unsafeApply[T, P], v => v.value)
  
  implicit val idSerDe: SerDe[ID] = getProductSerDe(ID.unapply, ID)
  implicit val emailSerde: SerDe[Email] = getProductSerDe(Email.unapply, Email)
  implicit val dateFormatSerDe: SerDe[DateFormat] = getProductSerDe(
    (dt: DateFormat) => Some(dt.pattern),
    (fmt: String) => DateFormat.fromString(fmt)
  )
  
  // SerDe's for metric parameters:
  implicit val approxDistinctParamsSerDe: SerDe[ApproxDistinctValuesParams] = getProductSerDe(
    p => Some(p.accuracyError),
    a => ApproxDistinctValuesParams(a)
  )
  
  // SerDe's for regular metrics configs:
//  implicit val rowCountMetricSerDe: SerDe[RowCountMetricConfig] =
//    getProductSerDe(RowCountMetricConfig.unapply, RowCountMetricConfig.tupled)
//  implicit val duplicateMetricSerDe: SerDe[DuplicateValuesMetricConfig] =
//    getProductSerDe(DuplicateValuesMetricConfig.unapply, DuplicateValuesMetricConfig.tupled)
//  implicit val distinctMetricSerDe: SerDe[DistinctValuesMetricConfig] =
//    getProductSerDe(DistinctValuesMetricConfig.unapply, DistinctValuesMetricConfig.tupled)
//  implicit val approxDistinctMetricSerDe: SerDe[ApproxDistinctValuesMetricConfig] =
//    getProductSerDe(ApproxDistinctValuesMetricConfig.unapply, ApproxDistinctValuesMetricConfig.tupled)
//    
//  def dummy[R <: RegularMetric](s: SerDe[R]): String = s.getClass.getName
//  
//  dummy(distinctMetricSerDe)
}
