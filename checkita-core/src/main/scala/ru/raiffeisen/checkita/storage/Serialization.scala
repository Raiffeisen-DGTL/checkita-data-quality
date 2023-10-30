package ru.raiffeisen.checkita.storage

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.json4s
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.{write, writePretty}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.storage.Models.DQEntity
import ru.raiffeisen.checkita.storage.Serialization.ResultsSerializationOps.unifiedSchema

import scala.reflect.runtime.universe._
import scala.util.Try

/** Results serialization */
object Serialization {
  
  /** Implicit conversion for results to enable their unified serialization. */
  implicit class ResultsSerializationOps[T <: DQEntity : TypeTag](value: T)(implicit settings: AppSettings) {

    /** Implicit Json4s formats */
    implicit val formats: DefaultFormats.type = DefaultFormats
    
    /**
     * Extract of field names from unified schema (not in order)
     */
    private lazy val unifiedFields: Set[String] = unifiedSchema.map(_.name).toSet

    /**
     * Results are designed to be saved into results storage and, therefore,
     * contains referenceDate and executionDate in form of timestamps.
     * For the purpose of the serialization timestamps are replaces with string
     * representation of referenceData and executionDate with pre-configured format.
     */
    private lazy val dateReplacement: Map[String, String] = Map(
      "referenceDate" -> settings.referenceDateTime.render,
      "executionDate" -> settings.executionDateTime.render
    )

    /**
     * Unified representation of result entity.
     * Result fields are separated into two categories:
     *   - top lvl fields that correspond to unified schema
     *   - data fields that holds information specific to this entity
     * 
     * All fields are serialized to json4s values (JValue type).
     * This enables correct string casting depending on the target format.
     */
    private lazy val unifiedRepr: Map[String, Any] = {
      val unifiedValues: Map[String, JValue] = fieldsValues.filter(f => unifiedFields.contains(f._1))
        .map(f => f._1 -> valueToJValue(f._2)).toMap

      val dataFields: Map[String, JValue] = fieldsValues.filterNot(f => unifiedFields.contains(f._1))
        .map(f => f._1 -> valueToJValue(f._2)).toMap

      unifiedValues ++ Map(
        "entityType" -> JString(value.entityType),
        "data" -> dataFields
      )
    }

    /**
     * Flat representation of result entity.
     * All fields are serialized to json4s values (JValue type). 
     * This enables correct string casting depending on the target format.
     */
    private lazy val flatRepr: Seq[(String, JValue)] = fieldsValues.map(f => f._1 -> valueToJValue(f._2))

    /**
     * Cast value to JValue.
     * @note Main idea here is that if string is a valid JSON string it will be parsed to JValue,
     *       otherwise it will be wrapped into JString.
     * @param value Value to cast to JValue
     * @return JValue
     */
    private def valueToJValue(value: Any): JValue = {
      val cast: Any => JValue = v => Try(parse(v.toString)).getOrElse(JString(v.toString))
      value match {
        case sq: Seq[_] => JArray(sq.map(cast).toList)
        case map: Map[_, _] => JObject(map.toSeq.map(t => JField(t._1.asInstanceOf[String], cast(t._2))) :_*)
        case Some(v) => cast(v)
        case None => JNothing
        case v => cast(v)
      }
    }

    /**
     * Converts JValue to a corresponding string.
     * @param value JValue to cast to string
     * @return String representation of JValue
     */
    private def JValueToString(value: Any): String = value match {
      case jStr: JString => jStr.extract[String]
      case jArr: JArray => write(jArr)
      case jObj: JObject => write(jObj)
      case jMap: Map[_, _] => write(jMap)
      case JNothing => "" // we will serialize None as empty string in flat representation.
      case otherJValue: JValue => Try(otherJValue.extract[String]).getOrElse(
        throw new IllegalArgumentException(s"Cannot serialize value '$otherJValue' to Json string.")
      )
      case nonJValue => throw new IllegalArgumentException(s"Cannot serialize '$nonJValue' to Json string because it is not a JValue.")
    }
    
    /**
     * Get case class field names in order they are defined.
     * @return Sequence of case class fields
     */
    private def getFields: Seq[String] = typeOf[T].members.sorted.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
    }

    /**
     * Retrieves sequence of tuples: fieldName -> fieldValue.
     * Sequence retain order in which were defined.
     * @return Sequence of tuples: fieldName -> fieldValue
     */
    private def fieldsValues: Seq[(String, Any)] =
      value.productIterator.toSeq.zip(getFields).map{
        case (_, n) if dateReplacement.contains(n) => n -> dateReplacement(n)
        case (v, n) => n -> v
      }

    /**
     * Serialize result entity into unified JSON string.
     * @return JSON string with result data
     */
    def toJson: String = writePretty(unifiedRepr)

    /**
     * Serialize result entity into unified Spark Row that match unified schema.
     * @note Data field contains Json string with data specific to given result entity.
     * @return Spark Row with result data
     */
    def toRow: Row = Row.fromSeq(
      unifiedSchema.map(c => JValueToString(unifiedRepr(c.name)))
    )

    /**
     * Serialize result entity into tab-separated string retaining order in which fields were defined.
     * @return Tab-separated strung with result data
     * @note This string has a flat structure and does not conform to unified schema.
     *       Used to generate attachments for notifications.
     *       Thus, tsv format with flat structure is more appropriated for visual inspection.
     */
    def toTsvString: String = flatRepr.map{
      case (_, value) => JValueToString(value)
    }.mkString("\t")

    /**
     * Gets flattened map of field name to string representation of field values.
     * @return Map fieldName -> fieldValue as string
     */
    def getFieldsMap: Map[String, String] = flatRepr.toMap.mapValues(JValueToString)
    
    /** Getter to retrieve TSV header (conforms to flattened tsv string) */
    def getTsvHeader: String = getFields.mkString("\t")
  }
  
  object ResultsSerializationOps {
    /**
     * Unified schema for any result entity. Schema includes of following fields:
     *   - jobId - ID of the job
     *   - referenceDate - Datetime for which this job was run
     *   - executionDate - Datetime when this job was run
     *   - entityType - Type of the result this record holds
     *   - data - Json string containing data specific to this result (entity type)
     */
    val unifiedSchema: StructType = StructType(Seq(
      StructField("jobId", StringType, nullable = true),
      StructField("referenceDate", StringType, nullable = true),
      StructField("executionDate", StringType, nullable = true),
      StructField("entityType", StringType, nullable = true),
      StructField("data", StringType, nullable = true)
    ))
  }
}
