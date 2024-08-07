package org.checkita.api.models

import io.circe.Encoder
import io.circe.parser._
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.storage.Models.DQEntity
import org.checkita.dqf.storage.Serialization._

import scala.reflect.runtime.universe.TypeTag

/**
 * Custom Json encoders that adds to or override default Circe encoders.
 */
object CustomEncoders {

  /**
   * Implicit conversion to generate custom Circe Json Encoder for DQ entities.
   * We want that API responses return the same JSON object as we generate when DQ jobs are run.
   *
   * @param settings Implicit application settings.
   * @tparam T Type of DQ entity
   * @return Circe Json Encoder for DQ entity of given type.
   */
  implicit def dqEntityEncoder[T <: DQEntity : TypeTag](implicit settings: AppSettings): Encoder[T] = 
    (a: T) => parse(a.toJson).fold(throw _, identity)
  
}
