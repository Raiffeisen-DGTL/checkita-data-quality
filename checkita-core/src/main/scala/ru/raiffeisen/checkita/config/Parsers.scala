package ru.raiffeisen.checkita.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PlanExpression}
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.internal.SQLConf
import pureconfig.error.{ConfigReaderFailure, ConvertFailure}

import java.io.{File, FileNotFoundException, InputStreamReader}
import scala.annotation.tailrec
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
   * Parsing utilities used to get list of all depended columns from unresolved Spark SQL expression.
   * This is required for Hive partition expression parsing in order to check if this expression
   * refers only to partitioning columns.
   *
   * @param ex Spark Column derived from SQL expression
   */
  implicit class ExpressionParsingOps(ex: Column) {

    /**
     * Tail recursive function to traverse Spark Expression children and collect all dependent column names.
     * In addition, this function will throw an exception in case if some sub-queries or other not allowed logic
     * is used inside expression for partitions filtering.
     *
     * @param expr  Sequence of top-level children from parent expression.
     * @param attrs Accumulator for storing found column names.
     * @return List of dependent column names for given expression.
     */
    @tailrec
    private def getAllColNames(expr: Seq[Expression], attrs: Seq[String] = Seq.empty): Seq[String] =
      if (expr.isEmpty) attrs
      else {
        expr.head match {
          case _: Literal => getAllColNames(expr.tail, attrs)
          case attr: UnresolvedAttribute => getAllColNames(expr.tail, attrs :+ attr.name)
          case func: UnresolvedFunction => getAllColNames(func.children ++ expr.tail, attrs)
          case _: PlanExpression[_] => throw new IllegalArgumentException(
            "Unable to parse expression to filter partitions: sub-queries are not allowed"
          )
          case otherExpr: Expression => getAllColNames(otherExpr.children ++ expr.tail, attrs)
          case _ => throw new IllegalArgumentException(
            "Unable to parse expression to filter partitions: unknown expression sub-type found. " +
              "Expression must contain only reference to partition column, " +
              "literals and SQL functions."
          )
        }
      }

    /**
     * Returns list of all dependent column names for this column expression.
     *
     * @return List of column names.
     */
    def dependentColumns: Seq[String] = getAllColNames(ex.expr.children)
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
