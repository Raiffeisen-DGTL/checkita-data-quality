package ru.raiffeisen.checkita.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import pureconfig.error.{ConfigReaderFailure, ConvertFailure}

import java.io.{File, FileNotFoundException, InputStreamReader}
import scala.util.{Failure, Success, Try}

object Parsers {

  /** SparkSqlParser used to validate IDs
   *
   * @note SparkSqlParser has evolved in Spark 3.1 to use active SQLConf.
   *       Thus, its constructor became parameterless.
   *       Therefore, in order to instantiate SparkSqlParser it is required
   *       to get constructor specific to current Spark version.
   */
  val idParser: SparkSqlParser = {
    val parserCls = classOf[SparkSqlParser]
    (Try(parserCls.getConstructor()), Try(parserCls.getConstructor(classOf[SQLConf]))) match {
      case (Success(constructor), Failure(_)) => constructor.newInstance()
      case (Failure(_), Success(constructor)) => constructor.newInstance(new SQLConf)
      case _ => throw new NoSuchMethodException("Unable to construct Spark SQL Parser")
    }
  }

  /**
   * Type class for Config parsers
   * @tparam T Type of the input from which the config should be parsed.
   */
  sealed trait ConfigParser[T] {
    def parse(input: T): Config
  }

  /**
   * Implicit config parser for string input
   */
  implicit object StringConfigParser extends ConfigParser[String] {
    override def parse(input: String): Config = {
      if (input.nonEmpty) ConfigFactory.parseString(input).resolve()
      else throw new IllegalArgumentException("Failed to read from provided configuration string: it's empty.")
    }
  }

  /**
   * Implicit config parser for file input
   */
  implicit object FileConfigParser extends ConfigParser[File] {
    override def parse(input: File): Config =
      if (input.exists) ConfigFactory.parseFile(input).resolve()
      else throw new FileNotFoundException(s"File ${input.getAbsoluteFile} does not exists.")
  }

  /**
   * Implicit config parser for input stream reader input
   */
  implicit object StreamReaderConfigParser extends ConfigParser[InputStreamReader] {
    override def parse(input: InputStreamReader): Config =
      if (input.ready) ConfigFactory.parseReader(input).resolve()
      else throw new IllegalArgumentException(
        "Failed to read from provided configuration stream: it's in 'not ready' state."
      )
  }

  /**
   * Implicit conversion for PureConfig reader failure in order to enhance it with
   * required string representation
   * @param f PureConfig reader failure.
   */
  implicit final class ConfigReaderFailureOps(f: ConfigReaderFailure) {
    private val tabs = (n: Int) => " " * 4 * n
    private val clearDescription = (s: String) => {
      val rgxRefined = " to eu\\.timepit.+?: ".r
      val rgxRefinedList = " '\\[.+]'".r
      val rgxCaseCls = " Note that the default transformation for representing class names.+$".r
      val clearRefined = rgxRefined.replaceAllIn(s.replace("\n", "~~~"), ": ")
      val clearRefinedList = rgxRefinedList.replaceAllIn(clearRefined, "")
      val cleared = rgxCaseCls.replaceAllIn(clearRefinedList, "").replace("~~~", "\n")
      if (cleared.endsWith("..")) cleared.dropRight(1) else cleared
    }
    private val prettifyDescription = (f: ConfigReaderFailure) =>
      clearDescription(f.description).split('\n').map(s => tabs(1) + s).mkString("\n")

    def prettify: String = f match {
      case convertFailure: ConvertFailure =>
        convertFailure.path + "\n" + prettifyDescription(convertFailure)
      case otherFailure => "path is unknown\n" + prettifyDescription(otherFailure)
    }
  }
}
