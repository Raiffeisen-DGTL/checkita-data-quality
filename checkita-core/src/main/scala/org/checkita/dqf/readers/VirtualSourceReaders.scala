package org.checkita.dqf.readers

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.checkita.dqf.appsettings.AppSettings
import org.checkita.dqf.config.jobconf.Sources._
import org.checkita.dqf.core.Source
import org.checkita.dqf.utils.ResultUtils._
import org.checkita.dqf.utils.SparkUtils.DataFrameOps

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
    protected def getParents(config: T, parentSources: Map[String, Source]): Seq[Source] =
      config.parents.map( sId => parentSources.getOrElse(
        sId, throw new NoSuchElementException(s"Parent source with id = '$sId' not found.")
      ))

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param parents       Sequence of parent sources for this virtual source
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Spark Dataframe
     */
    def getDataFrame(config: T, parents: Seq[Source], readMode: ReadMode)(implicit settings: AppSettings,
                                                                          spark: SparkSession): DataFrame

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
        val parents: Seq[Source] = getParents(config, parentSources)
        
        // checkpoint could be defined only for streaming applications 
        // There are only two type of virtual streams currently: 
        // select and filter. Both of them must have exactly one parent.
        // Thus we will use checkpoint of parent regular stream.
        val checkpoint = readMode match {
          case ReadMode.Batch => None
          case ReadMode.Stream => if (parents.size == 1) parents.head.checkpoint else None
        }
        
        val df = getDataFrame(config, parents, readMode)
        // persist if necessary:
        if (config.persist.nonEmpty) df.persist(config.persist.get)
        Source.validated(
          config.id.value, df, config.keyFields.map(_.value), config.parents, checkpoint
        )(settings.enableCaseSensitivity)
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
     * @param parents       Sequence of parent sources for this virtual source
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Spark Dataframe
     * @note SqlVirtualSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def getDataFrame(config: SqlVirtualSourceConfig,
                     parents: Seq[Source],
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession): DataFrame =
      if (settings.allowSqlQueries) {
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
     * @param parents       Sequence of parent sources for this virtual source
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Spark Dataframe
     * @note JoinVirtualSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def getDataFrame(config: JoinVirtualSourceConfig,
                     parents: Seq[Source],
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession): DataFrame = {
      require(parents.size == 2,
        s"Join virtual source must have two parent sources defined. Got following: " +
          parents.map(_.id).mkString("[", ",", "]")
      )
      val leftSrc = parents.head.df.as(parents.head.id)
      val rightSrc = parents.tail.head.df.as(parents.tail.head.id)

      leftSrc.join(rightSrc, config.joinBy.value, config.joinType.toString.toLowerCase)
    }
  }

  /** `FILTER` virtual source reader */
  implicit object FilterVirtualSourceReader extends VirtualSourceReader[FilterVirtualSourceConfig] {

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param parents       Sequence of parent sources for this virtual source
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Spark Dataframe
     */
    def getDataFrame(config: FilterVirtualSourceConfig,
                     parents: Seq[Source],
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession): DataFrame = {
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
     * @param parents       Sequence of parent sources for this virtual source
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Spark Dataframe
     */
    def getDataFrame(config: SelectVirtualSourceConfig,
                     parents: Seq[Source],
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession): DataFrame = {
      require(parents.size == 1,
        s"Select virtual source must have exactly one parent source. Got following: " +
          parents.map(_.id).mkString("[", ",", "]")
      )
      val parentDf = parents.head.df

      readMode match {
        case ReadMode.Batch => parentDf.select(config.expr.value :_*)
        case ReadMode.Stream => if (config.windowBy.isEmpty) {
          val allColumns = config.expr.value ++ Seq(
            settings.streamConfig.checkpointCol,
            settings.streamConfig.windowTsCol,
            settings.streamConfig.eventTsCol
          ).map(col)
          parentDf.select(allColumns: _*)
        } else {
          val allColumns = config.expr.value :+ col(settings.streamConfig.checkpointCol)
          parentDf.select(allColumns :_*).prepareStream(config.windowBy.get)
        }
      }
    }
  }

  /** `AGGREGATE` virtual source reader */
  implicit object AggregateVirtualSourceReader extends VirtualSourceReader[AggregateVirtualSourceConfig] {

    /**
     * Builds virtual source dataframe given the virtual source configuration.
     *
     * @param config        Virtual source configuration
     * @param parents       Sequence of parent sources for this virtual source
     * @param readMode      Mode in which source is read. Either 'batch' or 'stream'
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Spark Dataframe
     * @note AggregateVirtualSource is not streamable, therefore, 'readMode' argument is ignored
     *       and source is always read as static DataFrame.
     */
    def getDataFrame(config: AggregateVirtualSourceConfig,
                     parents: Seq[Source],
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession): DataFrame = {
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
     * @param parents       Sequence of parent sources for this virtual source
     * @param settings      Implicit application settings object
     * @param spark         Implicit spark session object
     * @return Spark Dataframe
     */
    def getDataFrame(config: VirtualSourceConfig,
                     parents: Seq[Source],
                     readMode: ReadMode)(implicit settings: AppSettings,
                                         spark: SparkSession): DataFrame =
    config match {
      case sql: SqlVirtualSourceConfig => SqlVirtualSourceReader.getDataFrame(sql, parents, readMode)
      case join: JoinVirtualSourceConfig => JoinVirtualSourceReader.getDataFrame(join, parents, readMode)
      case filter: FilterVirtualSourceConfig => FilterVirtualSourceReader.getDataFrame(filter, parents, readMode)
      case select: SelectVirtualSourceConfig => SelectVirtualSourceReader.getDataFrame(select, parents, readMode)
      case aggregate: AggregateVirtualSourceConfig => AggregateVirtualSourceReader.getDataFrame(aggregate, parents, readMode)
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
