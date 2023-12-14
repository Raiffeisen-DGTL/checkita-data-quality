package ru.raiffeisen.checkita.readers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.jobconf.Sources._
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.utils.ResultUtils._
import ru.raiffeisen.checkita.utils.SparkUtils.DataFrameOps

import scala.util.Try

object VirtualSourceReaders {
  
  /** Base virtual sources reader
   * @note In order to read virtual source it is required to provide map of already defined sources.
   */
  sealed trait VirtualSourceReader[T <: VirtualSourceConfig] {

    /**
     * Retrieves parent sources for a particular virtual source configuration.
     * @param config Virtual source configuration
     * @param parentSources Map of already defined virtual sources
     * @return Sequence of parent sources specific to given virtual source configuration.
     */
    protected def getParents(config: T)(implicit parentSources: Map[String, Source]): Seq[Source] =
      config.parents.map( sId => parentSources.getOrElse(
        sId, throw new NoSuchElementException(s"Parent source with id = '$sId' not found.")
      ))

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @return Spark Dataframe
     */
    def getDataFrame(config: T, readMode: ReadMode)(implicit settings: AppSettings,
                                                    spark: SparkSession,
                                                    parentSources: Map[String, Source]): DataFrame

    /**
     * Safely reads virtual source given source configuration.
     *
     * @param config        Virtual source configuration
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Either a valid Source or a list of source reading errors.
     */
    def read(config: T,
             parentSources: Map[String, Source],
             readMode: ReadMode)(implicit settings: AppSettings,
                                 spark: SparkSession): Result[Source] =
      Try{
        implicit val parents: Map[String, Source] = parentSources
        val df = getDataFrame(config, readMode)
        // persist if necessary:
        if (config.persist.nonEmpty) df.persist(config.persist.get)
        Source(config.id.value, df, config.keyFields.map(_.value), config.parents)
      }.toResult(
        preMsg = s"Unable to read virtual source '${config.id.value}' due to following error: "
      )
  }
  
  /** `SQL` virtual source reader */
  implicit object SqlVirtualSourceReader extends VirtualSourceReader[SqlVirtualSourceConfig] {

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @return Spark Dataframe
     * @note SqlVirtualSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def getDataFrame(config: SqlVirtualSourceConfig,
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession,
                                         parentSources: Map[String, Source]): DataFrame =
      if (settings.allowSqlQueries) {
        val parents = getParents(config)
        // register tempViews:
        parents.foreach(src => src.df.createOrReplaceTempView(src.id))
        spark.sql(config.query.value)
      } else throw new UnsupportedOperationException(
        "FORBIDDEN: Can't load SQL virtual source due to usage of arbitrary SQL queries is not allowed. " +
          "In order to use arbitrary sql queries set `allowSqlQueries` to true in application settings."
      )
  }

  /** `JOIN` virtual source reader */
  implicit object JoinVirtualSourceReader extends VirtualSourceReader[JoinVirtualSourceConfig] {

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @return Spark Dataframe
     * @note JoinVirtualSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def getDataFrame(config: JoinVirtualSourceConfig,
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession,
                                         parentSources: Map[String, Source]): DataFrame = {
      val parents = getParents(config)
      require(parents.size == 2,
        s"Join virtual source must have two parent sources defined. Got following: " +
          parents.map(_.id).mkString("[", ",", "]")
      )
      val leftSrc = parents.head.df
      val rightSrc = parents.tail.head.df

      leftSrc.join(rightSrc, config.joinBy.value, config.joinType.toString.toLowerCase)
    }
  }

  /** `FILTER` virtual source reader */
  implicit object FilterVirtualSourceReader extends VirtualSourceReader[FilterVirtualSourceConfig] {

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @return Spark Dataframe
     */
    def getDataFrame(config: FilterVirtualSourceConfig,
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession,
                                         parentSources: Map[String, Source]): DataFrame = {
      val parents = getParents(config)
      require(parents.size == 1,
        s"Filter virtual source must have exactly one parent source. Got following: " +
          parents.map(_.id).mkString("[", ",", "]")
      )
      val parentDf = parents.head.df

      readMode match {
        case ReadMode.Batch => parentDf.filter(config.expr.value.reduce(_ && _))
        case ReadMode.Stream =>
          if (config.windowBy.isEmpty) parentDf.filter(config.expr.value.reduce(_ && _))
          else parentDf
            .drop(settings.streamConfig.eventTsCol, settings.streamConfig.windowTsCol)
            .filter(config.expr.value.reduce(_ && _))
            .prepareStream(config.windowBy.get)
      }
    }
  }

  /** `SELECT` virtual source reader */
  implicit object SelectVirtualSourceReader extends VirtualSourceReader[SelectVirtualSourceConfig] {

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @return Spark Dataframe
     */
    def getDataFrame(config: SelectVirtualSourceConfig,
                     readMode: ReadMode)(implicit settings: AppSettings,
                                                        spark: SparkSession,
                                                        parentSources: Map[String, Source]): DataFrame = {
      val parents = getParents(config)
      require(parents.size == 1,
        s"Select virtual source must have exactly one parent source. Got following: " +
          parents.map(_.id).mkString("[", ",", "]")
      )
      val parentDf = parents.head.df

      readMode match {
        case ReadMode.Batch => parentDf.select(config.expr.value :_*)
        case ReadMode.Stream => if (config.windowBy.isEmpty) {
          val allColumns = config.expr.value ++ Seq(
            settings.streamConfig.windowTsCol,
            settings.streamConfig.eventTsCol
          ).map(col)
          parentDf.select(allColumns: _*)
        } else parentDf.select(config.expr.value :_*).prepareStream(config.windowBy.get)
      }
    }
  }

  /** `AGGREGATE` virtual source reader */
  implicit object AggregateVirtualSourceReader extends VirtualSourceReader[AggregateVirtualSourceConfig] {

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @return Spark Dataframe
     * @note AggregateVirtualSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def getDataFrame(config: AggregateVirtualSourceConfig,
                     readMode: ReadMode)(implicit settings: AppSettings,
                                                           spark: SparkSession,
                                                           parentSources: Map[String, Source]): DataFrame = {
      val parents = getParents(config)
      require(parents.size == 1,
        s"Aggregate virtual source must have exactly one parent source. Got following: " +
          parents.map(_.id).mkString("[", ",", "]")
      )
      val parentDf = parents.head.df
      val expr = config.expr.value
      
      parentDf.groupBy(config.groupBy.value.map(c => col(c)) :_* )
        .agg(expr.head, expr.tail :_*)
    }
  }

  /**
   * Generic virtual source reader that calls specific reader depending on the virtual source configuration type.
   */
  implicit object AnyVirtualSourceReader extends VirtualSourceReader[VirtualSourceConfig] {
    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @param parentSources Map of already defined sources (sourceId -> Source)
     * @return Spark Dataframe
     */
    def getDataFrame(config: VirtualSourceConfig,
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession,
                                         parentSources: Map[String, Source]): DataFrame =
    config match {
      case sql: SqlVirtualSourceConfig => SqlVirtualSourceReader.getDataFrame(sql, readMode)
      case join: JoinVirtualSourceConfig => JoinVirtualSourceReader.getDataFrame(join, readMode)
      case filter: FilterVirtualSourceConfig => FilterVirtualSourceReader.getDataFrame(filter, readMode)
      case select: SelectVirtualSourceConfig => SelectVirtualSourceReader.getDataFrame(select, readMode)
      case aggregate: AggregateVirtualSourceConfig => AggregateVirtualSourceReader.getDataFrame(aggregate, readMode)
      case other => throw new IllegalArgumentException(
        s"Unsupported virtual source type: '${other.getClass.getTypeName}"
      )
    }
  }

  /**
   * Implicit conversion for virtual source configurations to enable read and readStream methods for them.
   *
   * @param config   Virtual source configuration
   * @param reader   Implicit reader for given virtual source configuration
   * @param settings Implicit application settings object
   * @param spark    Implicit spark session object
   * @tparam T Type of virtual source configuration
   */
  implicit class VirtualSourceReaderOps[T <: VirtualSourceConfig](config: T)
                                                                 (implicit reader: VirtualSourceReader[T],
                                                                  settings: AppSettings,
                                                                  spark: SparkSession) {
    def read(parents: Map[String, Source]): Result[Source] = reader.read(config, parents, ReadMode.Batch)
    def readStream(parents: Map[String, Source]): Result[Source] =
      Try(if (!config.streamable) throw new UnsupportedOperationException(
        s"Virtual source '${config.id.value}' of kind '" +
          config.getClass.getSimpleName.dropRight("VirtualSourceConfig".length).toLowerCase +
          s"' is not streamable and, therefore, cannot be read as a stream."
      )).toResult(preMsg = s"Unable to read virtual source as a stream")
        .flatMap(_ => reader.read(config, parents, ReadMode.Stream))
  }
}
