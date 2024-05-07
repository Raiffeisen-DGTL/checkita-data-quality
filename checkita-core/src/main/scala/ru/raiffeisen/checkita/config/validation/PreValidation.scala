package ru.raiffeisen.checkita.config.validation

import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}
import ru.raiffeisen.checkita.config.Enums.TrendCheckRule
import ru.raiffeisen.checkita.config.appconf.StreamConfig
import ru.raiffeisen.checkita.config.jobconf.Checks._
import ru.raiffeisen.checkita.config.jobconf.MetricParams.LevenshteinDistanceParams
import ru.raiffeisen.checkita.config.jobconf.Sources._
import ru.raiffeisen.checkita.config.Parsers.ExpressionParsingOps
import ru.raiffeisen.checkita.config.jobconf.Schemas.RegistrySchemaConfig

import javax.validation.ValidationException
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success, Try}

// required import
import eu.timepit.refined.auto._
import eu.timepit.refined.pureconfig._
import ru.raiffeisen.checkita.config.Implicits._

/**
 * On read validations (on write validations will be added later)
 */
object PreValidation {

  /**
   * Ensure that registry schema is fetched either by it ID or by its subject, but not both.
   *
   * @param s Parsed registry schema configuration
   * @return Boolean validation result
   */
  private def registrySchemaValidation(s: RegistrySchemaConfig): Boolean =
    (s.schemaId.nonEmpty && s.schemaSubject.isEmpty) || (s.schemaId.isEmpty && s.schemaSubject.nonEmpty)

  /**
   * Implicit RegistrySchema reader validation
   */
  implicit val validateRegistrySchemaReader: ConfigReader[RegistrySchemaConfig] =
    deriveReader[RegistrySchemaConfig].ensure(
      registrySchemaValidation,
      _ => "Registry schema must be fetched from schema registry either by its ID or by its subject but not both."
    )

  /**
   * Implicit RegistrySchema writer validation
   */
  implicit val validateRegistrySchemaWriter: ConfigWriter[RegistrySchemaConfig] =
    deriveWriter[RegistrySchemaConfig].contramap[RegistrySchemaConfig]{ x =>
      if (registrySchemaValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "registry schema must be fetched from schema registry either by its ID or by its subject but not both."
      )
    }
  
  /**
   * Implicit StreamConfig reader validation
   */
  implicit val validateStreamConfigReader: ConfigReader[StreamConfig] =
    deriveReader[StreamConfig].ensure(
      s => s.trigger < s.window,
      _ => "For proper streaming processing, micro-batch trigger interval must be less than window interval."
    )

  /**
   * Implicit StreamConfig writer validation
   */
  implicit val validateStreamConfigWriter: ConfigWriter[StreamConfig] =
    deriveWriter[StreamConfig].contramap[StreamConfig] { x =>
      if (x.trigger < x.window) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "for proper streaming processing, micro-batch trigger interval must be less than window interval."
      )
    }

  /**
   * Implicit FixedFileSource reader validation
   */
  implicit val validateFixedFileSourceReader: ConfigReader[FixedFileSourceConfig] =
    deriveReader[FixedFileSourceConfig].ensure(
      _.schema.nonEmpty,
      _ => "For fixed-width files schema must always be specified."
    )

  /**
   * Implicit FixedFileSource writer validation
   */
  implicit val validateFixedFileSourceWriter: ConfigWriter[FixedFileSourceConfig] =
    deriveWriter[FixedFileSourceConfig].contramap[FixedFileSourceConfig] { x =>
      if (x.schema.nonEmpty) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: for fixed-width files schema must always be specified."
      )
    }

  /**
   * Ensure that schema for delimited files is either read from header or from explicit schema.
   * @param f Parsed delimited file source configuration
   * @return Boolean validation result
   */
  private def delimitedFileSourceValidation(f: DelimitedFileSourceConfig): Boolean = 
    (f.header && f.schema.isEmpty) || ( !f.header && f.schema.nonEmpty)
  
  /**
   * Implicit DelimitedFileSource reader validation
   */
  implicit val validateDelimitedFileSourceReader: ConfigReader[DelimitedFileSourceConfig] = 
    deriveReader[DelimitedFileSourceConfig].ensure(
      delimitedFileSourceValidation,
      _ => "For delimited files schema must either be read from header or from explicit schema but not from both."
    )

  /**
   * Implicit DelimitedFileSource writer validation
   */
  implicit val validateDelimitedFileSourceWriter: ConfigWriter[DelimitedFileSourceConfig] =
    deriveWriter[DelimitedFileSourceConfig].contramap[DelimitedFileSourceConfig]{ x => 
      if (delimitedFileSourceValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "for delimited files schema must either be read from header or from explicit schema but not both."
      )
    }

  /**
   * Ensure that table source has defined to read either entire table or query result but not both.
   * @param t Parsed table source configuration
   * @return Boolean validation result
   */
  private def tableSourceValidation(t: TableSourceConfig): Boolean =
    (t.table.nonEmpty && t.query.isEmpty) || (t.table.isEmpty && t.query.nonEmpty)
  
  implicit val validateTableSourceReader: ConfigReader[TableSourceConfig] =
    deriveReader[TableSourceConfig].ensure(
      tableSourceValidation,
      _ => "For table sources either table or query to read must be defined but not both."
    )
  
  implicit val validateTableSourceWriter: ConfigWriter[TableSourceConfig] =
    deriveWriter[TableSourceConfig].contramap[TableSourceConfig] { x =>
      if (tableSourceValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + 
          "for table sources either table or query to read must be defined but not both."
      )
    }

  /**
   * Ensure that Hive Partition definition is either includes implicitly defined list of values
   * or an SQL expression to filter partitions but not both.
   *
   * @param h Parsed hive partition configuration
   * @return Boolean validation result
   */
  private def hivePartitionDefValidation(h: HivePartition): Boolean =
    (h.values.nonEmpty && h.expr.isEmpty) || (h.values.isEmpty && h.expr.nonEmpty)

  /**
   * Ensures that in case if Hive Partition is defined with use of SQL expression
   * then this expression must contain only reference to partition column,
   * literals and sql functions.
   *
   * @param h Parsed hive partition configuration
   * @return Boolean validation result as well as sequence of error message (in case of failed validation).
   */
  private def hivePartitionExprValidation(h: HivePartition): (Boolean, Seq[String]) =
    if (h.expr.isEmpty) (true, Seq.empty)
    else Try(h.expr.get.dependentColumns) match {
      case Success(cols) =>
        val invalidColRefs = cols.filterNot(_ == h.name.value)
        if (invalidColRefs.isEmpty) (true, Seq.empty)
        else (false, Seq(
          "Expression to filter partitions must not contain reference to other columns.",
          "Found following invalid column references:",
          invalidColRefs.mkString("'", ", ", "'") + ".",
          "If you are using SQL functions without arguments, please call them with empty parentheses,",
          "e.g. `current_date()` but not `current_date`."
        ))
      case Failure(e) => (false, Seq(e.getMessage))
    }

  /**
   * Implicit HivePartition reader validation
   */
  implicit val validateHivePartitionReader: ConfigReader[HivePartition] = deriveReader[HivePartition]
    .ensure(
      hivePartitionDefValidation,
      _ => "Hive partitions to read must be defined either explicitly as a list of partition values " +
        "or as a SQL filter expression but not both."
    )
    .ensure(
      h => hivePartitionExprValidation(h)._1,
      h => hivePartitionExprValidation(h)._2.mkString(" ")
    )

  /**
   * Implicit HivePartition writer validation
   */
  implicit val validateHivePartitionWriter: ConfigWriter[HivePartition] =
    deriveWriter[HivePartition].contramap[HivePartition]{ x =>
      if (hivePartitionDefValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "Hive partitions to read must be defined either explicitly as a list of partition values " +
          "or as a SQL filter expression but not both."
      )
    }.contramap[HivePartition]{ x =>
      val (isValid, errors) = hivePartitionExprValidation(x)
      if (isValid) x else throw new ValidationException(s"Error during writing ${x.toString}: " + errors.mkString(" "))
    }


  /**
   * Ensure that kafka source configuration contains at exactly on of the topic definitions.
   * @param k Parsed kafka source configuration
   * @return Boolean validation result
   */
  private def kafkaSourceTopicsValidation(k: KafkaSourceConfig): Boolean =
    (k.topics.isEmpty & k.topicPattern.nonEmpty) | (k.topics.nonEmpty & k.topicPattern.isEmpty)

  /**
   * Ensure that kafka source configuration contains sequence of topics in the same notation:
   * either with partitions to read or without them.
   * @param k Parsed kafka source configuration
   * @return Boolean validation result
   */
  private def kafkaTopicsFormatValidation(k: KafkaSourceConfig): Boolean =
    k.topics.forall(_.value.contains('@')) |
      k.topics.forall(!_.value.contains('@'))

  /**
   * Implicit KafkaSource reader validation
   */
  implicit val validateKafkaSourceReader: ConfigReader[KafkaSourceConfig] = deriveReader[KafkaSourceConfig]
    .ensure(
      kafkaSourceTopicsValidation,
      _ => 
        "Kafka topics must be defined explicitly as a sequence of topic names or as a topic pattern. " +
          "Either 'topics' or 'topicPattern' must be defined but not both."
    )
    .ensure(
      kafkaTopicsFormatValidation,
      _ =>
        "Mixed topic notation: all topics must be defined either with partitions to read " + 
          " or without them (read all topic partitions)."
    )

  /**
   * Implicit KafkaSource writer validation
   */
  implicit val validateKafkaSourceWriter: ConfigWriter[KafkaSourceConfig] =
    deriveWriter[KafkaSourceConfig].contramap[KafkaSourceConfig]{ x => 
      if (kafkaSourceTopicsValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "kafka topics must be defined explicitly as a sequence of topic names or as a topic pattern. " +
          "Either 'topics' or 'topicPattern' must be defined but not both."
      )
    }.contramap[KafkaSourceConfig]{ x => 
      if (kafkaTopicsFormatValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "Mixed topic notation: all topics must be defined either with partitions to read " +
          " or without them (read all topic partitions)."
      )
    }
  
  /**
   * Ensure that parameters for levenshteinDistance metric are correct.
   * @param p Parsed parameters for levenshteinDistance metric
   * @return Boolean validation result
   */
  private def lvnstDistParamsValidation(p: LevenshteinDistanceParams): Boolean = {
    val normalizedValidation = p.normalize & p.threshold >= 0.0 & p.threshold <= 1.0
    val nonNormalizedValidation = !p.normalize & (p.threshold.toInt == p.threshold)
    
    normalizedValidation | nonNormalizedValidation
  }

  /**
   * Implicit LevenshteinDistanceParams reader validation
   */
  implicit val validateLevenshteinDistanceParamsReader: ConfigReader[LevenshteinDistanceParams] =
    deriveReader[LevenshteinDistanceParams].ensure(
      lvnstDistParamsValidation,
      m => "levenshteinDistance column metric requires that threshold be " + {
        if (m.normalize)
          "a double within [0, 1] interval if 'normalize' parameter is set to true."
        else
          "an integer if 'normalize' parameter is set to false."
      }
    )

  /**
   * Implicit LevenshteinDistanceParams writer validation
   */
  implicit val validateLevenshteinDistanceParamsWriter: ConfigWriter[LevenshteinDistanceParams] =
    deriveWriter[LevenshteinDistanceParams].contramap[LevenshteinDistanceParams]{ x => 
      if (lvnstDistParamsValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "levenshteinDistance column metric requires that threshold be " + {
            if (x.normalize)
              "a double within [0, 1] interval if 'normalize' parameter is set to true."
            else
              "an integer if 'normalize' parameter is set to false."
          }
      )
    }
  
  /**
   * Gets check name as written in configuration from it class name
   * @param c Parsed check configuration
   * @return Check name
   */
  private def getCheckName(c: CheckConfig): String =
    c.getClass.getSimpleName.dropRight("Check".length)
      .zipWithIndex.map(t => if (t._2 == 0) t._1.toLower else t._1).mkString
  
  /**
   * Ensure that differByLT check has both compareMetric and threshold parameters defined.
   * @param c Parsed differByLT check configuration
   * @return Boolean validation result
   */
  private def differByLtCheckValidation(c: DifferByLtCheckConfig): Boolean =
    c.threshold.nonEmpty & c.compareMetric.nonEmpty

  /**
   * Implicit DifferByLtCheck reader validation
   */
  implicit val validateDifferByLtCheckReader: ConfigReader[DifferByLtCheckConfig] =
    deriveReader[DifferByLtCheckConfig].ensure(
      differByLtCheckValidation,
      _ => "differByLT check requires that both 'compareMetric' and 'threshold' parameters be defined."
    )

  /**
   * Implicit DifferByLtCheck writer validation
   */
  implicit val validateDifferByLtCheckWriter: ConfigWriter[DifferByLtCheckConfig] =
    deriveWriter[DifferByLtCheckConfig].contramap[DifferByLtCheckConfig] { x =>
      if (differByLtCheckValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " +
          "differByLT check requires that both 'compareMetric' and 'threshold' parameters be defined."
      )
    }
    
  /**
   * Ensure that snapshot check configuration (except differByLT) contains 
   * either threshold or compareMetric parameter but not both.
   * @param c Parsed snapshot check configuration
   * @return Boolean validation result
   */
  private def snapShotCheckValidation(c: SnapshotCheckConfig): Boolean =
    (c.threshold.isEmpty & c.compareMetric.nonEmpty) | (c.threshold.nonEmpty & c.compareMetric.isEmpty)

  /**
   * Generates error message when snapshot check validation fails.
   * @param s Parsed snapshot check configuration
   * @return Validation error message
   */
  private def snapShotCheckValidationMsg(s: SnapshotCheckConfig): String =
    getCheckName(s) + 
      " check requires that either 'threshold' or 'compareMetric' parameter be set but not both."
    
    
  /**
   * Implicit EqualToCheck reader validation
   */
  implicit val validateEqualToCheckReader: ConfigReader[EqualToCheckConfig] =
    deriveReader[EqualToCheckConfig].ensure(
      snapShotCheckValidation,
      snapShotCheckValidationMsg
    )

  /**
   * Implicit EqualToCheck writer validation
   */
  implicit val validateEqualToCheckWriter: ConfigWriter[EqualToCheckConfig] =
    deriveWriter[EqualToCheckConfig].contramap[EqualToCheckConfig] { x =>
      if (snapShotCheckValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + snapShotCheckValidationMsg(x)
      )
    }
      
  /**
   * Implicit LessThanCheck reader validation
   */
  implicit val validateLessThanCheckReader: ConfigReader[LessThanCheckConfig] =
    deriveReader[LessThanCheckConfig].ensure(
      snapShotCheckValidation,
      snapShotCheckValidationMsg
    )

  /**
   * Implicit LessThanCheck writer validation
   */
  implicit val validateLessThanCheckWriter: ConfigWriter[LessThanCheckConfig] =
    deriveWriter[LessThanCheckConfig].contramap[LessThanCheckConfig] { x =>
      if (snapShotCheckValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + snapShotCheckValidationMsg(x)
      )
    }
    
  /**
   * Implicit GreaterThanCheck reader validation
   */
  implicit val validateGreaterThanCheckReader: ConfigReader[GreaterThanCheckConfig] =
    deriveReader[GreaterThanCheckConfig].ensure(
      snapShotCheckValidation,
      snapShotCheckValidationMsg
    )

  /**
   * Implicit GreaterThanCheck writer validation
   */
  implicit val validateGreaterThanCheckWriter: ConfigWriter[GreaterThanCheckConfig] =
    deriveWriter[GreaterThanCheckConfig].contramap[GreaterThanCheckConfig] { x =>
      if (snapShotCheckValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + snapShotCheckValidationMsg(x)
      )
    }
    
  /**
   * Ensure that average bound checks windowSize and windowOffset parameters comply with rule:
   *   - if rule is record then windowSize and windowOffset must be non-negative integers
   *   - if rule is datetime then windowSize and windowOffset must be valid non-negative durations
   * @param t Parsed average bound check configuration
   * @return Boolean validation result
   */
  private def trendCheckWindowValidation(t: TrendCheckConfig with AverageBoundCheckConfig): Boolean = {
    val windowSize = t.windowSize.value
    if (t.rule == TrendCheckRule.Record) {
      val windowOffset = t.windowOffset.map(_.value).getOrElse("0")
      Seq(windowSize, windowOffset).forall { w =>
        Try(w.toInt).map(i => assert(i >= 0)).isSuccess
      }
    } else {
      val windowOffset = t.windowOffset.map(_.value).getOrElse("0s")
      Seq(windowSize, windowOffset).forall { w =>
        Try(Duration(w)).map(d => assert(d >= Duration("0s"))).isSuccess
      }
    }
  }

  /**
   * Generates error message when average bound check windowSize and windowOffset validation fails.
   * @param t Parsed average bound check configuration
   * @return Validation error message
   */
  private def trendCheckWindowValidationMsg(t: TrendCheckConfig with AverageBoundCheckConfig): String = {
    getCheckName(t) + " check requires that windowSize and windowOffset parameters " + {
      if (t.rule == TrendCheckRule.Record)
        "be non-negative integers when rule is set to 'record'"
      else
        "be valid non-negative durations when rule is set to 'datetime'"
    }
  }

  /**
   * Implicit AverageBoundFullCheck reader validation
   */
  implicit val validateAverageBoundFullCheckReader: ConfigReader[AverageBoundFullCheckConfig] =
    deriveReader[AverageBoundFullCheckConfig].ensure(
      trendCheckWindowValidation,
      trendCheckWindowValidationMsg
    )
    
  /**
   * Implicit AverageBoundFullCheck writer validation
   */
  implicit val validateAverageBoundFullCheckWriter: ConfigWriter[AverageBoundFullCheckConfig] =
    deriveWriter[AverageBoundFullCheckConfig].contramap[AverageBoundFullCheckConfig] { x =>
      if (trendCheckWindowValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + trendCheckWindowValidationMsg(x)
      )
    }
    
  /**
   * Implicit AverageBoundLowerCheck reader validation
   */
  implicit val validateAverageBoundLowerCheckReader: ConfigReader[AverageBoundLowerCheckConfig] =
    deriveReader[AverageBoundLowerCheckConfig].ensure(
      trendCheckWindowValidation,
      trendCheckWindowValidationMsg
    )

  /**
   * Implicit AverageBoundLowerCheck writer validation
   */
  implicit val validateAverageBoundLowerCheckWriter: ConfigWriter[AverageBoundLowerCheckConfig] =
    deriveWriter[AverageBoundLowerCheckConfig].contramap[AverageBoundLowerCheckConfig] { x =>
      if (trendCheckWindowValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + trendCheckWindowValidationMsg(x)
      )
    }
    
  /**
   * Implicit AverageBoundUpperCheck reader validation
   */
  implicit val validateAverageBoundUpperCheckReader: ConfigReader[AverageBoundUpperCheckConfig] =
    deriveReader[AverageBoundUpperCheckConfig].ensure(
      trendCheckWindowValidation,
      trendCheckWindowValidationMsg
    )

  /**
   * Implicit AverageBoundUpperCheck writer validation
   */
  implicit val validateAverageBoundUpperCheckWriter: ConfigWriter[AverageBoundUpperCheckConfig] =
    deriveWriter[AverageBoundUpperCheckConfig].contramap[AverageBoundUpperCheckConfig] { x =>
      if (trendCheckWindowValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + trendCheckWindowValidationMsg(x)
      )
    }
    
  /**
   * Implicit AverageBoundRangeCheck reader validation
   */
  implicit val validateAverageBoundRangeCheckReader: ConfigReader[AverageBoundRangeCheckConfig] =
    deriveReader[AverageBoundRangeCheckConfig].ensure(
      trendCheckWindowValidation,
      trendCheckWindowValidationMsg
    )

  /**
   * Implicit AverageBoundRangeCheck writer validation
   */
  implicit val validateAverageBoundRangeCheckWriter: ConfigWriter[AverageBoundRangeCheckConfig] =
    deriveWriter[AverageBoundRangeCheckConfig].contramap[AverageBoundRangeCheckConfig] { x =>
      if (trendCheckWindowValidation(x)) x
      else throw new ValidationException(
        s"Error during writing ${x.toString}: " + trendCheckWindowValidationMsg(x)
      )
    }
  
}
