package ru.raiffeisen.checkita.core.metrics.serialization

trait SerializersTuples extends SerDeTransformations {
  
  /**
   * Implicit conversion to generate SerDe for Tuple2[T1, T2].
   */
  implicit def tuple2SerDe[T1, T2](implicit serDeT1: SerDe[T1], serDeT2: SerDe[T2]): SerDe[(T1, T2)] =
    union(serDeT1, serDeT2)

  /**
   * Implicit conversion to generate SerDe for Tuple[T1, T2, T3]
   */
  implicit def tuple3SerDe[T1, T2, T3](implicit serDeT12: SerDe[(T1, T2)], serDeT3: SerDe[T3]): SerDe[(T1, T2, T3)] = {
    val unionSerDe: SerDe[((T1, T2), T3)] = union(serDeT12, serDeT3)
    val toTuple3: (((T1, T2), T3)) => (T1, T2, T3) = t => (t._1._1, t._1._2, t._2)
    val fromTuple3: ((T1, T2, T3)) => ((T1, T2), T3) = t => ((t._1, t._2), t._3)
    transform(unionSerDe, toTuple3, fromTuple3)
  }

  /**
   * Implicit conversion to generate SerDe for Tuple[T1, T2, T3, T4]
   */
  implicit def tuple4SerDe[T1, T2, T3, T4](implicit serDeT123: SerDe[(T1, T2, T3)], 
                                           serDeT4: SerDe[T4]): SerDe[(T1, T2, T3, T4)] = {
    val unionSerDe: SerDe[((T1, T2, T3), T4)] = tuple2SerDe(serDeT123, serDeT4)
    val toTuple4: (((T1, T2, T3), T4)) => (T1, T2, T3, T4) = t => (t._1._1, t._1._2, t._1._3, t._2)
    val fromTuple4: ((T1, T2, T3, T4)) => ((T1, T2, T3), T4) = t => ((t._1, t._2, t._3), t._4)
    transform(unionSerDe, toTuple4, fromTuple4)
  }

  /**
   * Implicit conversion to generate SerDe for Tuple[T1, T2, T3, T4, T5]
   */
  implicit def tuple5SerDe[T1, T2, T3, T4, T5](implicit serDeT1234: SerDe[(T1, T2, T3, T4)],
                                               serDeT5: SerDe[T5]): SerDe[(T1, T2, T3, T4, T5)] = {
    val unionSerDe: SerDe[((T1, T2, T3, T4), T5)] = tuple2SerDe(serDeT1234, serDeT5)
    val toTuple5: (((T1, T2, T3, T4), T5)) => (T1, T2, T3, T4, T5) = t => (t._1._1, t._1._2, t._1._3, t._1._4, t._2)
    val fromTuple5: ((T1, T2, T3, T4, T5)) => ((T1, T2, T3, T4), T5) = t => ((t._1, t._2, t._3, t._4), t._5)
    transform(unionSerDe, toTuple5, fromTuple5)
  }

  /**
   * Implicit conversion to generate SerDe for Tuple[T1, T2, T3, T4, T5]
   */
  implicit def tuple6SerDe[T1, T2, T3, T4, T5, T6](implicit serDeT12345: SerDe[(T1, T2, T3, T4, T5)],
                                                   serDeT6: SerDe[T6]): SerDe[(T1, T2, T3, T4, T5, T6)] = {
    val unionSerDe: SerDe[((T1, T2, T3, T4, T5), T6)] = tuple2SerDe(serDeT12345, serDeT6)
    val toTuple6: (((T1, T2, T3, T4, T5), T6)) => (T1, T2, T3, T4, T5, T6) = 
      t => (t._1._1, t._1._2, t._1._3, t._1._4, t._1._5, t._2)
    val fromTuple6: ((T1, T2, T3, T4, T5, T6)) => ((T1, T2, T3, T4, T5), T6) = 
      t => ((t._1, t._2, t._3, t._4, t._5), t._6)
    transform(unionSerDe, toTuple6, fromTuple6)
  }
}
