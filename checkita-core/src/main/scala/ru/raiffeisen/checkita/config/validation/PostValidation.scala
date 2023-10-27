package ru.raiffeisen.checkita.config.validation

import com.typesafe.config.{Config, ConfigFactory, ConfigList, ConfigMergeable, ConfigObject, ConfigOrigin, ConfigRenderOptions, ConfigValue, ConfigValueType}
import pureconfig.error.{ConfigReaderFailure, ConvertFailure, UserValidationFailed}
import ru.raiffeisen.checkita.utils.FormulaParser
import ru.raiffeisen.checkita.utils.Templating.{getTokens, renderTemplate}

import java.util
import java.util.Map
import scala.annotation.tailrec
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.util.{Random, Try}

object PostValidation {

  /**
   * Case class to represent configuration field
   * @param name Name of the configuration field
   * @param value Value of the configuration field
   * @param path Path to a configuration field from the root
   */
  final case class Field(name: String, value: Any, path: String)

  /**
   * Appends field name or index to a path
   * @param path Current path
   * @param value Field name/index to append
   * @return Updated path
   */
  private def appendToPath(path: String, value: String): String =
    if (path.isEmpty) value else path + "." + value

  /**
   * Gets sub-config object from given config object provided with path.
   * If such object does not exists, returns empty object.
   * @param cObj Top-level config object
   * @param path Path to retrieve sub-config object
   * @return Config object by path or empty config object (if path does not exists)
   */
  private def getObjOrEmpty(cObj: ConfigObject, path: String): ConfigObject =
    Try(cObj.toConfig.getObject(path)).getOrElse(ConfigFactory.empty().root())
    
  /**
   * Implicit conversion for ConfigValue to implement desirable value rendering.
   * @param v ConfigValue to render with predefined options
   */
  private implicit class ConfigValueOpts(v: ConfigValue) {

    val renderOpts: ConfigRenderOptions = ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)
    
    val trimQuotes: String => String = s =>
      if (s.startsWith("\"\"\"") & s.endsWith("\"\"\"")) s.substring(3, s.length - 3)
      else if (s.startsWith("\"") & s.endsWith("\"")) s.substring(1, s.length - 1)
      else s
    
    def renderWithOpts: String = trimQuotes(v.render(renderOpts))
  }
  
  /**
   * Tail recursive DFS for all configuration values with given field name
   * @param root Root configuration object
   * @param lookupField Field name to search values for
   * @param pathPrefix Path prefix (for cases when searching for sub-configurations)
   * @return Sequence of all configuration values with requested field name
   */
  private def getAllValues(root: ConfigObject,
                           lookupField: String,
                           pathPrefix: String = ""): Seq[Field] = {

    @tailrec
    def dfs(stack: Seq[Field],
            visited: Set[Field] = Set.empty,
            acc: Seq[Field] = Seq.empty): Seq[Field] = stack match {
      case Nil => acc
      case head :: rest =>
        if (visited.contains(head)) {
          dfs(rest, visited, acc)
        } else {
          head.value match {
            case entity: ConfigObject =>
              val fields = entity.asScala.toList.map(t => Field(t._1, t._2, appendToPath(head.path, t._1)))
              val unvisited = fields.filterNot(visited.contains)
              dfs(unvisited ++ rest, visited + head, acc)
            case seq: ConfigList =>
              val unvisited = seq.asScala.toList.zipWithIndex.map{ t =>
                Field(s"${head.name}@${t._2}", t._1, appendToPath(head.path, t._2.toString))
              }.filterNot(visited.contains)
              dfs(unvisited ++ rest, visited + head, acc)
            case value: ConfigValue =>
              if (head.name.split("@").head == lookupField) {
                val strVal = value.renderWithOpts
                dfs(rest, visited + head, acc :+ Field(head.name, strVal, head.path))
              } else
                dfs(rest, visited + head, acc)
          }
        }
    }

    val initFields = root.asScala.toList.map(t => Field(t._1, t._2, appendToPath(pathPrefix, t._1)))
    dfs(initFields)
  }

  /**
   * Common function to validate cross reference between different configuration objects.
   * @param checkObj Configuration object to check for wrong references
   * @param refObj Configuration object that should contain all the referenced fields.
   * @param checkField Field name that contain reference
   * @param refField Field name referenced in checked configuration object
   * @param msgTemplate Error message template 
   * @param checkPathPrefix Path prefix for checked configuration object
   * @param refPathPrefix Path prefix for referenced configuration object
   * @param checkPathFilter Optional function to filter out some fields from checked object by their path
   * @param refPathFilter Optional function to filter out some fields from referenced object by their path
   * @return Sequence of config reader failures
   */
  private def validateCrossReferences(
                                       checkObj: ConfigObject,
                                       refObj: ConfigObject,
                                       checkField: String,
                                       refField: String,
                                       msgTemplate: String,
                                       checkPathPrefix: String = "jobConfig",
                                       refPathPrefix: String = "jobConfig",
                                       checkPathFilter: String => Boolean = (_: String) => true,
                                       refPathFilter: String => Boolean = (_: String) => true
                                     ): Vector[ConfigReaderFailure] = {
    val checkFields = getAllValues(checkObj, checkField, checkPathPrefix)
      .filter(f => checkPathFilter(f.path))
    val refFields = getAllValues(refObj, refField, refPathPrefix)
      .filter(f => refPathFilter(f.path)).map(_.value.toString).toSet
    
    checkFields.filterNot(f => refFields.contains(f.value.toString)).map{ f =>
      ConvertFailure(
        UserValidationFailed(msgTemplate.format(f.value.toString)),
        None,
        f.path
      )
    }.toVector
  }
  
  /**
   * Validation to check if DQ job configuration contains duplicate IDs.
   */
  val validateIds: ConfigObject => Vector[ConfigReaderFailure] = root =>
    getAllValues(root, "id")
      .groupBy(_.value.toString).filter(t => t._2.length > 1)
      .map { t =>
        val conflictWith = t._2.tail.map(_.path).mkString("[", ", ", "]")
        ConvertFailure(
          UserValidationFailed(
            s"Duplicate IDs are found with value '${t._1}'. Conflicts with IDs at: $conflictWith"
          ),
          None,
          t._2.head.path
        )
      }.toVector

  /**
   * Validation to check if DQ job configuration contains missing references
   * from table sources to jdbc connections
   */
  val validateJdbcSourceRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.sources"),
      getObjOrEmpty(root, "jobConfig.connections"),
      "connection",
      "id",
      "Table source refers to undefined JDBC connection '%s'",
      checkPathPrefix = "jobConfig.sources",
      refPathPrefix = "jobConfig.connections",
      checkPathFilter = (s: String) => s.contains(".table."),
      refPathFilter = (s: String) => !s.contains(".kafka.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from kafka topics sources to kafka connections
   */
  val validateKafkaSourceRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.sources"),
      getObjOrEmpty(root, "jobConfig.connections"),
      "connection",
      "id",
      "Kafka topic source refers to undefined Kafka connection '%s'",
      checkPathPrefix = "jobConfig.sources",
      refPathPrefix = "jobConfig.connections",
      checkPathFilter = (s: String) => s.contains(".kafka."),
      refPathFilter = (s: String) => s.contains(".kafka.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from sources to schemas
   */
  val validateSourceSchemaRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.sources"),
      getObjOrEmpty(root, "jobConfig"),
      "schema",
      "id",
      "Source refers to undefined schema '%s'",
      checkPathPrefix = "jobConfig.sources",
      checkPathFilter = (s: String) => !s.contains(".hive."),
      refPathFilter = (s: String) => s.startsWith("jobConfig.schemas.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from virtual sources to already defined sources.
   * Check is recursive: virtual sources can also refer to other virtual sources defined above.
   */
  val validateVirtualSourceRefs: ConfigObject => Vector[ConfigReaderFailure] = root => {
    
    val sPathPrefix = "jobConfig.sources"
    val vsPathPrefix = "jobConfig.virtualSources"
    val refKey = "parentSources"
    
    @tailrec
    def loop(vs: Seq[(ConfigObject, Int)],
             sIds: Set[String],
             acc: Vector[ConfigReaderFailure] = Vector.empty): Vector[ConfigReaderFailure] = vs match {
      case Nil => acc
      case head :: tail =>
        val vsId = head._1.get("id").renderWithOpts
        val errors = head._1.toConfig.getStringList(refKey).asScala.zipWithIndex
          .filterNot(ps => sIds.contains(ps._1)).map { ps =>
            ConvertFailure(
              UserValidationFailed(
                s"Virtual source refers to undefined source '${ps._1}'"
              ),
              None,
              s"$vsPathPrefix.${head._2}.$refKey.${ps._2}"
            )
          }
        loop(tail, sIds + vsId, acc ++ errors)
    }
    
    val sourceIds = getAllValues(
      getObjOrEmpty(root, sPathPrefix), "id", "jobConfig"
    ).map(_.value.toString).toSet
    val virtualSources = Try(
      root.toConfig.getObjectList(vsPathPrefix).asScala.toList.zipWithIndex
    ).getOrElse(List.empty)
      
    loop(virtualSources, sourceIds)
  }
  
  /**
   * Validation to check if DQ job configuration contains missing references
   * from source metrics to sources
   */
  val validateSourceMetricRefs: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val allSourceIds = (
      getAllValues(getObjOrEmpty(root,"jobConfig.sources"), "id", "jobConfig") ++
      Try(root.toConfig.getObjectList("jobConfig.virtualSources").asScala).getOrElse(List.empty)
        .flatMap(getAllValues(_, "id", "jobConfig"))
    ).map(_.value.toString).toSet
    
    val allMetricSourceRefs = getAllValues(
        getObjOrEmpty(root, "jobConfig.metrics.regular"),
        "source", 
        "jobConfig.metrics.regular"
      )

    allMetricSourceRefs.filterNot(f => allSourceIds.contains(f.value.toString)).map{ f =>
      ConvertFailure(
        UserValidationFailed(s"Metric refers to undefined source '${f.value.toString}'"),
        None,
        f.path
      )
    }.toVector
  }

  /**
   * Validation to check if DQ job configuration contains errors in composed metric formulas:
   *   - missing reference to source metrics
   *   - wrong equation that can be parsed
   */
  val validateComposedMetrics: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val compMetPrefix = "jobConfig.metrics.composed"
    val sourceMetricIds = getAllValues(
      getObjOrEmpty(root, "jobConfig.metrics.regular"), 
      "id", "jobConfig.metrics.regular"
    ).map(f => f.value.toString).toSet
    
    Try(root.toConfig.getObjectList(compMetPrefix).asScala).getOrElse(List.empty).zipWithIndex.flatMap { compMet => 
      val formula = compMet._1.get("formula").renderWithOpts
      val comMetPath = s"$compMetPrefix.${compMet._2}.formula"
      val mIds = getTokens(formula)
      val mResMap = mIds.map(_ -> Random.nextDouble.toString).toMap
      val parsedFormula = renderTemplate(formula, mResMap)
      val p = new FormulaParser{} // anonymous class
      
      mIds.filterNot(sourceMetricIds.contains).map{ m =>
        ConvertFailure(
          UserValidationFailed(s"Composed metric formula refers to undefined metric '$m'"),
          None,
          comMetPath
        )
      } ++ Seq(parsedFormula).filter(f => Try(p.eval(p.parseAll(p.expr, f).get)).isFailure).map{ _ =>
        ConvertFailure(
          UserValidationFailed(s"Cannot parse composed metric formula '$formula'"),
          None,
          comMetPath
        )
      }
    }.toVector
  }

  /**
   * Validation to check if DQ job configuration contains missing references
   * from load checks to sources
   */
  val validateLoadCheckRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.loadChecks"),
      getObjOrEmpty(root, "jobConfig"),
      "source",
      "id",
      "Load check refers to undefined source '%s'",
      checkPathPrefix = "jobConfig.loadChecks",
      refPathFilter = (s: String) => s.startsWith("jobConfig.sources.") || s.startsWith("jobConfig.virtualSources.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from schema match load checks to schemas
   */
  val validateLoadCheckSchemaRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.loadChecks"),
      getObjOrEmpty(root, "jobConfig"),
      "schema",
      "id",
      "Load check refers to undefined schema '%s'",
      checkPathPrefix = "jobConfig.loadChecks",
      checkPathFilter = (s: String) => s.contains(".schemaMatch."),
      refPathFilter = (s: String) => s.startsWith("jobConfig.schemas.")
    )
  
  /**
   * Validation to check if DQ job configuration contains missing references
   * from snapshot checks to sources
   */
  val validateSnapshotCheckRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.checks.snapshot"),
      getObjOrEmpty(root, "jobConfig.metrics"),
      "metric",
      "id",
      "Snapshot check refers to undefined metric '%s'",
      checkPathPrefix = "jobConfig.checks.snapshot"
    ) ++ validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.checks.snapshot"),
      getObjOrEmpty(root, "jobConfig.metrics"),
      "compareMetric",
      "id",
      "Snapshot check refers to undefined metric '%s'",
      checkPathPrefix = "jobConfig.checks.snapshot"
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from trend checks to sources
   */
  val validateTrendCheckRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.checks.trend"),
      getObjOrEmpty(root, "jobConfig.metrics"),
      "metric",
      "id",
      "Trend check refers to undefined metric '%s'",
      checkPathPrefix = "jobConfig.checks.trend"
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from targets to connections
   */
  val validateTargetConnectionRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.targets"),
      getObjOrEmpty(root, "jobConfig.connections"),
      "connection",
      "id",
      "Target refers to undefined connection '%s'",
      checkPathPrefix = "jobConfig.targets"
    )
  
  /**
   * Validation to check if DQ job configuration contains missing references
   * from targets to metrics
   */
  val validateTargetMetricRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.targets"),
      getObjOrEmpty(root, "jobConfig.metrics"),
      "metrics",
      "id",
      "Target refers to undefined metric '%s'",
      checkPathPrefix = "jobConfig.targets"
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from targets to checks
   */
  val validateTargetCheckRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.targets"),
      getObjOrEmpty(root, "jobConfig.checks"),
      "checks",
      "id",
      "Target refers to undefined check '%s'",
      checkPathPrefix = "jobConfig.targets"
    )
  
  /**
   * All post validations for Data Quality job-level configuration
   */
  val allPostValidations: Seq[ConfigObject => Vector[ConfigReaderFailure]] = Vector(
    validateIds,
    validateJdbcSourceRefs,
    validateKafkaSourceRefs,
    validateSourceSchemaRefs,
    validateVirtualSourceRefs,
    validateSourceMetricRefs,
    validateComposedMetrics,
    validateLoadCheckRefs,
    validateLoadCheckSchemaRefs,
    validateSnapshotCheckRefs,
    validateTrendCheckRefs,
    validateTargetConnectionRefs,
    validateTargetMetricRefs,
    validateTargetCheckRefs
  )
}
