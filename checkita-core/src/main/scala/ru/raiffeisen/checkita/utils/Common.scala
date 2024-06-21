package ru.raiffeisen.checkita.utils

import enumeratum.EnumEntry
import eu.timepit.refined.api.Refined
import org.apache.commons.io.FileUtils.openInputStream
import org.apache.commons.io.IOUtils.toInputStream
import org.json4s.DefaultFormats
import ru.raiffeisen.checkita.config.RefinedTypes.DateFormat
import ru.raiffeisen.checkita.utils.ResultUtils._

import java.io.{File, InputStreamReader, SequenceInputStream, Serializable}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.jdk.CollectionConverters._
import scala.collection.compat.immutable.ArraySeq
import scala.reflect.runtime.universe._
import scala.util.Try

object Common {

  implicit val jsonFormats: DefaultFormats.type = DefaultFormats

  /**
   * Converts camelCase name into snake_case name
   * @param name camelCase name to convert
   * @return snake_case name
   */
  def camelToSnakeCase(name: String): String =
    "[A-Z\\d]".r.replaceAllIn(name, { m =>
      if (m.end(0) == 1) {
        m.group(0).toLowerCase()
      } else {
        "_" + m.group(0).toLowerCase()
      }
    })

  /**
   * Builds MD5 hash for input string.
   * @param s String to build has for.
   * @return MD5 hash string.
   */
  def getStringHash(s: String): String =
    MessageDigest.getInstance("MD5").digest(s.getBytes).map("%02x".format(_)).mkString
  
  /**
   * Converts sequence of parameter strings with format "k=v" into a Map(k -> v)
   * @param paramsSeq Sequence of parameter strings
   * @return Parameters Map
   */
  def paramsSeqToMap(paramsSeq: Seq[String]): Map[String, String] =
    paramsSeq.map(_.split("=", 2)).collect{
      case Array(k, v) => k -> v
    }.toMap

  /**
   * Collects field values of case class in form of Map(field_name -> field_value).
   * Also unpacks Refined and DateFormat fields if any (required for proper JSON serialization)
   * @param obj Case class instance to get field values from
   * @tparam T Generic type constrained to case classes
   * @return Map of case class fields with values.
   */
  def getFieldsMap[T <: Product with Serializable : TypeTag](obj: T): Map[String, Any] = {
    val fields = typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
    }.toSet
    obj.getClass.getDeclaredFields
      .filter(f => fields.contains(f.getName))  // retain only fields that are case class fields.
      .foldLeft(Map.empty[String, Any]){ (m, f) =>
        f.setAccessible(true)
        val value = f.get(obj).asInstanceOf[Any] match {
          case v: Refined[_, _] => v.value
          case v: DateFormat => v.pattern
          case v: EnumEntry => v.toString
          case v => v
        }
        m + (f.getName -> value)
      }
  }

  /**
   * Retrieves DQ variables from environment, combines them with explicitly provided variables
   * and then converts them into a string that will be prepended to a configuration prior reading.
   *
   * @param extraVariables Explicitly provided extra variables.
   * @return String with variables to be prepended to configuration.
   *
   * @note Variables are collected from system and java-runtime environment and retained only those that are related to
   *       Checkita Data Quality Framework: following regex is used to find such variables: `^(?i)(DQ)[a-z0-9_-]+$`.
   *       These variables are combined with the ones provided in `extraVariables` argument and further used during
   *       configuration files parsing.
   */
  def getPrependVars(extraVariables: Map[String, String]): String = {
    val variablesRegex = "^(?i)(DQ)[a-z0-9_-]+$" // system variables must match the given regex.
    val systemVariables = System.getProperties.asScala.filter{ case (k, _) => k matches variablesRegex }
    (systemVariables ++ extraVariables).map {
      case (k, v) => k + ": \"" + v + "\""
    }.mkString("", "\n", "\n")
  }

  /**
   * Prepares configuration files for reading. The idea here is following:
   *   - system and user-provided variables are need to be prepended to the configuration file(s)
   *   - HOCON support configuration merging, and, therefore, we also allows multiple files for input.
   *   - In order to merge variables and multiple configuration files, the sequence of input streams is created.
   * @param configs Sequence of configuration files for parsing
   *                (order maters! please, refer to HOCON specifications for more details)
   * @param prependVars System and user-defined variables to be used in confuguration files.
   * @param confName Name of configuration being read (either 'application' or 'job')
   * @return Either configuration input stream reader ready for parsing or a list of occurred errors.
   */
  def prepareConfig(configs: Seq[String], prependVars: String, confName: String): Result[InputStreamReader] = 
    Try {
      val vars = if (prependVars.isEmpty) "// no extra variables are provided.\n" else prependVars
      new InputStreamReader(configs.foldLeft(toInputStream(vars, StandardCharsets.UTF_8)){ (stream, config) =>
        new SequenceInputStream(stream, openInputStream(new File(config)))
      })
    }.toResult(preMsg = s"Unable to prepare $confName configuration for reading with following error:")
    
  
  implicit class UnsafeArraySeqOps[T](value: Array[T]) {
    def toSeqUnsafe: ArraySeq[T] = ArraySeq.unsafeWrapArray(value)
  }
}
