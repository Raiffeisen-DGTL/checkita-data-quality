package org.checkita.dqf.config.validation

import com.typesafe.config._
import pureconfig.error.{ConfigReaderFailure, ConvertFailure, UserValidationFailed}
import org.checkita.dqf.utils.FormulaParser
import org.checkita.dqf.utils.Templating.{getTokens, renderTemplate}

import scala.annotation.tailrec
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.{Random, Try}

object PostValidation {

  /**
   * Case class to represent configuration field
   *
   * @param name  Name of the configuration field
   * @param value Value of the configuration field
   * @param path  Path to a configuration field from the root
   */
  final case class Field(name: String, value: Any, path: String)

  /**
   * Class to represent cross reference used in configuration.
   *
   * @param value Reference value
   * @param path  Reference location
   */
  final class Reference(val value: String, val path: String) {
    /**
     * References are the same when their value is the same.
     * Path usually differs as cross-references are defined in different locations of configuration
     *
     * @param obj Object to compare with.
     * @return Equality check result
     */
    override def equals(obj: Any): Boolean = obj match {
      case ref: Reference => this.value == ref.value
      case _ => false
    }

    override def toString: String = s"Reference($value, $path)"

    override def hashCode(): Int = value.hashCode
  }

  /**
   * Appends field name or index to a path
   *
   * @param path  Current path
   * @param value Field name/index to append
   * @return Updated path
   */
  private def appendToPath(path: String, value: String): String =
    if (path.isEmpty) value else path + "." + value

  /**
   * Gets sub-config object from given config object provided with path.
   * If such object does not exists, returns empty object.
   *
   * @param cObj Top-level config object
   * @param path Path to retrieve sub-config object
   * @return Config object by path or empty config object (if path does not exists)
   */
  private def getObjOrEmpty(cObj: ConfigObject, path: String): ConfigObject =
    Try(cObj.toConfig.getObject(path)).getOrElse(ConfigFactory.empty().root())

  /**
   * Implicit conversion for ConfigValue to implement desirable value rendering.
   *
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
   *
   * @param root        Root configuration object
   * @param lookupField Field name to search values for
   * @param pathPrefix  Path prefix (for cases when searching for sub-configurations)
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
              val unvisited = seq.asScala.toList.zipWithIndex.map { t =>
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
   * Tail recursive DFS to find cycles in directed graph stored in adjacent list format: 
   * Map(node -> List(adjacent node))
   *
   * @param graph Graph to traverse for cycles.
   * @tparam N Type of the graph nodes.
   * @return List of cyclic paths in the graph.
   */
  def findCycles[N](graph: Map[N, List[N]]): List[List[N]] = {

    /**
     * Case class for storing DFS stack.
     *
     * @param node     Current node.
     * @param visited  Set of visited nodes.
     * @param recStack Recursive stack (set of nodes along a single path).
     * @param path     Current path.
     */
    case class Stack(node: N, visited: Set[N], recStack: Set[N], path: List[N])
    object Stack {
      /**
       * Initializes stack for starting node.
       *
       * @param node Starting node.
       * @return Initializes stack.
       */
      def init(node: N): Stack = Stack(node, Set.empty, Set.empty, List.empty)
    }

    /**
     * Traverse DFS from starting node.
     *
     * @param stack  Current stack state.
     * @param cycles List of found cycles.
     * @return List of found cycles.
     */
    @tailrec
    def dfs(stack: List[Stack], cycles: List[List[N]]): List[List[N]] = stack match {
      case Nil => cycles
      case Stack(curNode, visited, recStack, path) :: rest =>
        if (recStack.contains(curNode)) {
          val startIdx = path.indexOf(curNode)
          val cycle = path.drop(startIdx) :+ curNode
          dfs(rest, cycles :+ cycle)
        } else if (visited.contains(curNode)) {
          dfs(rest, cycles)
        }
        else {
          val neighbours = graph.getOrElse(curNode, List.empty)
          val newStack = neighbours.map(n => Stack(n, visited + curNode, recStack + curNode, path :+ curNode)) ++ rest
          dfs(newStack, cycles)
        }
    }

    graph.keys.foldLeft(List.empty[List[N]])(
      (acc, node) => dfs(List(Stack.init(node)), acc)
    ).groupBy(_.toSet).map { case (_, v) => v.head }.toList
  }

  /**
   * Common function to validate cross reference between different configuration objects.
   *
   * @param checkObj        Configuration object to check for wrong references
   * @param refObj          Configuration object that should contain all the referenced fields.
   * @param checkField      Field name that contain reference
   * @param refField        Field name referenced in checked configuration object
   * @param msgTemplate     Error message template 
   * @param checkPathPrefix Path prefix for checked configuration object
   * @param refPathPrefix   Path prefix for referenced configuration object
   * @param checkPathFilter Optional function to filter out some fields from checked object by their path
   * @param refPathFilter   Optional function to filter out some fields from referenced object by their path
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

    checkFields.filterNot(f => refFields.contains(f.value.toString)).map { f =>
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
   * Validation to check if DQ job configuration contains both
   * batch and streaming sources defined.
   */
  val validateBatchOrStream: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val sources = getObjOrEmpty(root, "jobConfig.sources")
    val streams = getObjOrEmpty(root, "jobConfig.streams")
    val virtualSources = Try(
      root.toConfig.getObjectList("jobConfig.virtualSources").asScala.toList
    ).getOrElse(List.empty)
    val virtualStreams = Try(
      root.toConfig.getObjectList("jobConfig.virtualStreams").asScala.toList
    ).getOrElse(List.empty)

    val isValid = (sources.isEmpty && virtualSources.isEmpty) || (streams.isEmpty && virtualStreams.isEmpty)

    if (isValid) Vector.empty
    else Vector(ConvertFailure(
      UserValidationFailed(
        s"Job configuration must be written either for batch or streaming application i.e. " +
          "it should contain either sources and virtualSources or streams and virtualStreams defined " +
          "but not both batch and streaming sources."
      ),
      None, "jobConfig"
    ))
  }

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
      refPathFilter = (s: String) => !s.contains(".kafka.") && !s.contains(".greenplum.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from kafka topics sources to kafka connections
   */
  val validateKafkaSourceRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig"),
      getObjOrEmpty(root, "jobConfig.connections"),
      "connection",
      "id",
      "Kafka topic source refers to undefined Kafka connection '%s'",
      refPathPrefix = "jobConfig.connections",
      checkPathFilter = (s: String) => s.contains(".sources.kafka.") || s.contains(".streams.kafka."),
      refPathFilter = (s: String) => s.contains(".kafka.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from table sources to pivotal connections
   */
  val validateGreenplumSourceRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig.sources"),
      getObjOrEmpty(root, "jobConfig.connections"),
      "connection",
      "id",
      "Greenplum source refers to undefined pivotal greenplum connection '%s'",
      checkPathPrefix = "jobConfig.sources",
      refPathPrefix = "jobConfig.connections",
      checkPathFilter = (s: String) => s.contains(".greenplum."),
      refPathFilter = (s: String) => s.contains(".greenplum.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from sources to schemas
   */
  val validateSourceSchemaRefs: ConfigObject => Vector[ConfigReaderFailure] = root =>
    validateCrossReferences(
      getObjOrEmpty(root, "jobConfig"),
      getObjOrEmpty(root, "jobConfig"),
      "schema",
      "id",
      "Source refers to undefined schema '%s'",
      checkPathFilter = (s: String) => (s.contains(".sources.") || s.contains(".streams.")) && !s.contains(".hive."),
      refPathFilter = (s: String) => s.startsWith("jobConfig.schemas.")
    )

  /**
   * Validation to check if DQ job configuration contains missing references
   * from virtual sources to already defined sources.
   * Check is recursive: virtual sources can also refer to other virtual sources defined above.
   *
   * @note batch and streaming sources are checked separately.
   */
  val validateVirtualSourceRefs: ConfigObject => Vector[ConfigReaderFailure] = root => {

    val sourcePathPrefix = "jobConfig.sources"
    val virtualSourcePathPrefix = "jobConfig.virtualSources"
    val streamPathPrefix = "jobConfig.streams"
    val virtualStreamPathPrefix = "jobConfig.virtualStreams"
    val refKey = "parentSources"

    def getVsData(vsPathPrefix: String): Seq[(String, Seq[(String, Int)], String, String)] =
      Try(root.toConfig.getObjectList(vsPathPrefix).asScala).getOrElse(Seq.empty)
        .zipWithIndex.map(vs => (
          vs._1.get("id").renderWithOpts,
          vs._1.toConfig.getStringList(refKey).asScala.zipWithIndex.toSeq,
          s"$vsPathPrefix.${vs._2}.id",
          s"$vsPathPrefix.${vs._2}.$refKey"
        )).toSeq

    def getMissingRefs(vsData: Seq[(String, Seq[(String, Int)], String, String)],
                       allIds: Set[String],
                       kind: String): Seq[ConvertFailure] = vsData.flatMap { vs =>
      vs._2.filterNot(ref => allIds.contains(ref._1)).map { missing =>
        ConvertFailure(
          UserValidationFailed(s"Virtual $kind refers to undefined $kind '${missing._1}'"),
          None,
          s"${vs._4}.${missing._2}"
        )
      }
    }

    def vsDataToRefGraph(vsData: Seq[(String, Seq[(String, Int)], String, String)]): Map[Reference, List[Reference]] = {
      val references = for {
        (id, refs, idPath, refPath) <- vsData
        (refId, refIdx) <- refs
      } yield new Reference(id, idPath) -> new Reference(refId, s"$refPath.$refIdx")

      references.groupBy(_._1).map { case (k, v) => k -> v.map(_._2).toList }
    }

    val sourceIds = getAllValues(
      getObjOrEmpty(root, sourcePathPrefix), "id", "jobConfig"
    ).map(_.value.toString).toSet
    val streamIds = getAllValues(
      getObjOrEmpty(root, streamPathPrefix), "id", "jobConfig"
    ).map(_.value.toString).toSet
    val virtualSources = getVsData(virtualSourcePathPrefix)
    val virtualStreams = getVsData(virtualStreamPathPrefix)

    val allSrcIds = sourceIds ++ virtualSources.map(_._1).toSet
    val allStrIds = streamIds ++ virtualStreams.map(_._1).toSet

    val missingVsRefs =
      getMissingRefs(virtualSources, allSrcIds, "source") ++ getMissingRefs(virtualStreams, allStrIds, "stream")

    if (missingVsRefs.nonEmpty) missingVsRefs.toVector
    else {
      val vSourceRefGraph = vsDataToRefGraph(virtualSources)
      val vStreamRefGraph = vsDataToRefGraph(virtualStreams)

      (findCycles(vSourceRefGraph).map((_, "source")) ++ findCycles(vStreamRefGraph).map((_, "stream"))).map {
        case (cycle, kind) => ConvertFailure(
          UserValidationFailed(
            s"Virtual $kind references form a cycle: " + cycle.map(f => f.value + "@" + f.path).mkString(" --> ")
          ),
          None,
          cycle.head.path
        )
      }.toVector
    }
  }

  /**
   * Validation to check if DQ job configuration contains non-streamable kinds of
   * virtual sources defined in virtualStreams list.
   */
  val validateVirtualStreams: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val virtualStreamKinds = Try(
      root.toConfig.getObjectList("jobConfig.virtualStreams").asScala.zipWithIndex
    ).getOrElse(List.empty).flatMap {
      case (obj, idx) => getAllValues(obj, "kind", s"jobConfig.virtualStreams.$idx")
    }
    val streamableKinds = Set("filter", "select")

    virtualStreamKinds
      .filter(f => !streamableKinds.contains(f.value.toString))
      .map(f => ConvertFailure(
        UserValidationFailed(s"Virtual source of kind '${f.value.toString}' is not streamable"),
        None,
        f.path
      )).toVector
  }

  /**
   * Validation to check if DQ job configuration contains missing references
   * from regular metrics to sources
   */
  val validateRegularMetricRefs: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val allSourceIds = (
      getAllValues(getObjOrEmpty(root, "jobConfig.sources"), "id", "jobConfig.sources") ++
        getAllValues(getObjOrEmpty(root, "jobConfig.streams"), "id", "jobConfig.streams") ++
        Try(root.toConfig.getObjectList("jobConfig.virtualSources").asScala).getOrElse(List.empty)
          .flatMap(getAllValues(_, "id", "jobConfig.virtualSources")) ++
        Try(root.toConfig.getObjectList("jobConfig.virtualStreams").asScala).getOrElse(List.empty)
          .flatMap(getAllValues(_, "id", "jobConfig.virtualStreams"))
      ).map(_.value.toString).toSet

    val allMetricSourceRefs = getAllValues(
      getObjOrEmpty(root, "jobConfig.metrics.regular"),
      "source",
      "jobConfig.metrics.regular"
    )

    allMetricSourceRefs.filterNot(f => allSourceIds.contains(f.value.toString)).map { f =>
      ConvertFailure(
        UserValidationFailed(s"Metric refers to undefined source '${f.value.toString}'"),
        None,
        f.path
      )
    }.toVector
  }

  /**
   * Validation to check if DQ job configuration contains errors in composed metric formulas:
   * wrong equation that cannot be parsed
   */
  val validateComposedMetrics: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val compMetPrefix = "jobConfig.metrics.composed"

    Try(root.toConfig.getObjectList(compMetPrefix).asScala).getOrElse(List.empty).zipWithIndex.flatMap { compMet =>
      val formula = compMet._1.get("formula").renderWithOpts
      val comMetPath = s"$compMetPrefix.${compMet._2}.formula"
      val mIds = getTokens(formula)
      val mResMap = mIds.map(_ -> Random.nextDouble().toString).toMap
      val parsedFormula = renderTemplate(formula, mResMap)
      val p = new FormulaParser {} // anonymous class

      Seq(parsedFormula).filter(f => Try(p.evalArithmetic(f)).isFailure).map { _ =>
        ConvertFailure(
          UserValidationFailed(s"Cannot parse composed metric formula '$formula'"),
          None,
          comMetPath
        )
      }
    }.toVector
  }

  /**
   * Validation to check if composed and trend metrics definitions does not create reference cycles.
   * Metric computation must be acyclic.
   */
  val validateMetricCrossReferences: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val compMetPrefix = "jobConfig.metrics.composed"
    val trendMetPrefix = "jobConfig.metrics.trend"

    val regularMetricIds = getAllValues(
      getObjOrEmpty(root, "jobConfig.metrics.regular"),
      "id", "jobConfig.metrics.regular"
    ).map(f => f.value.toString).toSet

    val composedMetricsData =
      Try(root.toConfig.getObjectList(compMetPrefix).asScala).getOrElse(List.empty).zipWithIndex.map(compMet => (
        compMet._1.get("id").renderWithOpts,
        getTokens(compMet._1.get("formula").renderWithOpts).distinct,
        s"$compMetPrefix.${compMet._2}.id",
        s"$compMetPrefix.${compMet._2}.formula"
      ))

    val trendMetricsData =
      Try(root.toConfig.getObjectList(trendMetPrefix).asScala).getOrElse(List.empty).zipWithIndex.map(trendMet => (
        trendMet._1.get("id").renderWithOpts,
        Seq(trendMet._1.get("lookupMetric").renderWithOpts),
        s"$trendMetPrefix.${trendMet._2}.id",
        s"$trendMetPrefix.${trendMet._2}.lookupMetric"
      ))

    // to check for missing references:
    val allIds = regularMetricIds ++ composedMetricsData.map(_._1).toSet ++ trendMetricsData.map(_._1).toSet

    val missingReferencesErrors = (composedMetricsData ++ trendMetricsData).flatMap { m =>
      m._2.filterNot(allIds.contains).map { missing =>
        ConvertFailure(
          UserValidationFailed(
            if (m._4.endsWith("formula")) s"Composed metric formula refers to undefined metric '$missing'"
            else s"Trend metric refers to undefined lookup metric '$missing'"
          ),
          None,
          m._4
        )
      }
    }

    // If missing references are found then return them as a validation errors,
    // otherwise check for cycles within references.
    if (missingReferencesErrors.nonEmpty) missingReferencesErrors.toVector
    else {
      val allRefs = for {
        (id, refs, idPath, refPath) <- composedMetricsData ++ trendMetricsData
        ref <- refs
      } yield new Reference(id, idPath) -> new Reference(ref, refPath)

      val graph = allRefs.groupBy(_._1).map { case (k, v) => k -> v.map(_._2).toList }

      findCycles(graph).map(cycle =>
        ConvertFailure(
          UserValidationFailed(
            s"Metric references form a cycle: " + cycle.map(f => f.value + "@" + f.path).mkString(" --> ")
          ),
          None,
          cycle.head.path
        )
      ).toVector
    }
  }

  /**
   * Ensure that windowSize and windowOffset parameters in trend metrics do comply with rule:
   *   - if rule is record then windowSize and windowOffset must be non-negative integers
   *   - if rule is datetime then windowSize and windowOffset must be valid non-negative durations
   *
   * @note This check is run at post-validation stage, as it is quite difficult to derive 
   *       pureconfig readers for sealed trait families (thus approach is used to define kinded configurations)
   *       to run check during config parsing.
   */
  val validateTrendMetricWindowSettings: ConfigObject => Vector[ConfigReaderFailure] = root =>
    Try(root.toConfig.getObjectList("jobConfig.metrics.trend").asScala).getOrElse(List.empty).zipWithIndex.flatMap {
      case (tm, idx) =>
        val rule = tm.toConfig.getString("rule")
        val wSize = tm.toConfig.getString("windowSize")
        val wOffset = Try(tm.toConfig.getString("windowOffset")).toOption
        val isValid =
          if (rule.toLowerCase == "record") Seq(wSize, wOffset.getOrElse("0")).forall { w =>
            Try(w.toInt).map(i => assert(i >= 0)).isSuccess
          } else Seq(wSize, wOffset.getOrElse("0s")).forall { w =>
            Try(Duration(w)).map(d => assert(d >= Duration("0s"))).isSuccess
          }
        if (isValid) Seq.empty else Seq(ConvertFailure(
          UserValidationFailed(
            "Trend metric requires that windowSize and windowOffset parameters " + {
              if (rule.toLowerCase == "record") "be non-negative integers when rule is set to 'record'"
              else "be valid non-negative durations when rule is set to 'datetime'"
            }
          ),
          None,
          s"jobConfig.metrics.trend.$idx.${tm.get("id").renderWithOpts}"
        ))
    }.toVector

  /**
   * Ensure validity of ARIMA model order parameters in ARIMA trend metric configuration.
   *
   * @note This check is run at post-validation stage, as it is quite difficult to derive 
   *       pureconfig readers for sealed trait families (thus approach is used to define kinded configurations)
   *       to run check during config parsing.
   */
  val validateArimaTrendMetricOrder: ConfigObject => Vector[ConfigReaderFailure] = root =>
    Try(root.toConfig.getObjectList("jobConfig.metrics.trend").asScala)
      .getOrElse(List.empty).zipWithIndex
      .filter(c => c._1.toConfig.getString("kind").toLowerCase == "arima")
      .flatMap {
        case (tm, idx) =>
          val order = tm.toConfig.getStringList("order").asScala.toSeq.map(_.toInt)
          val validations = Seq(
            (
              (p: Int, d: Int, q: Int) => Seq(p, d, q).exists(_ < 0),
              "ARIMA model order parameters must be non-negative."
            ),
            (
              (p: Int, _: Int, q: Int) => p + q <= 0,
              "Either AR (p) or MA (q) order of ARIMA model (or both) must be non-zero."
            )
          )
          order match {
            case Seq(p, d, q) => validations.filter(_._1(p, d, q)).map{
              case (_, msg) => ConvertFailure(
                UserValidationFailed(msg),
                None,
                s"jobConfig.metrics.trend.$idx.${tm.get("id").renderWithOpts}"
              )
            }
          }
      }.toVector
  
  /**
   * Validation to check if DQ job configuration contains missing references
   * from load checks to sources
   *
   * @note Load checks are not applied to streaming sources
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
   * from snapshot checks to metrics
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
   * from trend checks to metrics
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
   * Validation to check if DQ job configuration contains errors in expression check formulas:
   *   - missing reference to metrics
   *   - wrong equation that cannot be parsed
   */
  val validateExpressionCheckRefs: ConfigObject => Vector[ConfigReaderFailure] = root => {
    val exprChkPrefix = "jobConfig.checks.expression"
    val allMetIds = getAllValues(
      getObjOrEmpty(root, "jobConfig.metrics"),
      "id", "jobConfig.metrics"
    ).map(f => f.value.toString).toSet
    
    Try(root.toConfig.getObjectList(exprChkPrefix).asScala).getOrElse(List.empty).zipWithIndex.flatMap { exprChk =>
      val formula = exprChk._1.get("formula").renderWithOpts
      val exprChkPath = s"$exprChkPrefix.${exprChk._2}.formula"
      val mIds = getTokens(formula)
      val mResMap = mIds.map(_ -> Random.nextDouble().toString).toMap
      val parsedFormula = renderTemplate(formula, mResMap)
      val p = new FormulaParser {} // anonymous class

      mIds.filterNot(allMetIds.contains).map{ m =>
        ConvertFailure(
          UserValidationFailed(s"Expression check formula refers to undefined metric '$m'"),
          None,
          exprChkPath
        )
      } ++ Seq(parsedFormula).filter(f => Try(p.evalBoolean(f)).isFailure).map { _ =>
        ConvertFailure(
          UserValidationFailed(s"Cannot parse expression check formula '$formula'"),
          None,
          exprChkPath
        )
      }
    }.toVector
  }
  
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
    validateBatchOrStream,
    validateJdbcSourceRefs,
    validateKafkaSourceRefs,
    validateGreenplumSourceRefs,
    validateSourceSchemaRefs,
    validateVirtualStreams,
    validateVirtualSourceRefs,
    validateRegularMetricRefs,
    validateComposedMetrics,
    validateMetricCrossReferences,
    validateTrendMetricWindowSettings,
    validateArimaTrendMetricOrder,
    validateLoadCheckRefs,
    validateLoadCheckSchemaRefs,
    validateSnapshotCheckRefs,
    validateTrendCheckRefs,
    validateExpressionCheckRefs,
    validateTargetConnectionRefs,
    validateTargetMetricRefs,
    validateTargetCheckRefs
  )
}
