package ru.raiffeisen.checkita.sources

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.utils.{DQSettings, log}

import scala.collection.convert.ImplicitConversions.`collection asJava`

object VirtualSourceProcessor {
  def getActualSources(initialVirtualSourcesMap: Map[String, VirtualFile], initialSourceMap: Map[String, Source])
                      (implicit sparkSes: SparkSession, settings: DQSettings): Map[String, Source] = {

    @scala.annotation.tailrec
    def loop(virtualSourcesMap: Map[String, VirtualFile], actualSourcesMapAccumulator: Map[String, Source])
            (implicit sparkSes: SparkSession): Map[String, Source] = {

      log.info(s"Virtual sources to load: ${virtualSourcesMap.size}")

      if (virtualSourcesMap.isEmpty) {
        log.info(s"[SUCCESS] Virtual sources loading is complete.")
        actualSourcesMapAccumulator
      } else {
        val firstLevelVirtualSources: Map[String, VirtualFile] =
          virtualSourcesMap.filter {
            case (sourceId, conf: VirtualFile) =>
              val parentIds = conf.parentSourceIds
              log.info(s"  * Virtual source $sourceId | parents: ${parentIds.mkString(", ")}")
              actualSourcesMapAccumulator.keySet.containsAll(parentIds)
          }

        val otherSources: Map[String, Source] = firstLevelVirtualSources
          .map {
            case (vid, virtualFile) =>
              virtualFile match {
                case VirtualFileSelect(id, parentSourceIds, sqlCode, keyFields, save, persist) =>
                  val firstParent = parentSourceIds.head
                  log.info(s"Processing '$id', type: 'FILTER-SQL', parent: '$firstParent'")
                  log.info(s"SQL: $sqlCode")
                  val dfSource = actualSourcesMapAccumulator.get(firstParent).head

                  dfSource.df.createOrReplaceTempView(firstParent)
                  val preDf = sparkSes.sql(sqlCode)
                  val virtualSourceDF = preDf.select(preDf.columns.map(c => col(c).as(c.toLowerCase)) : _*)

                  //persist feature
                  if (persist.isDefined) {
                    virtualSourceDF.persist(persist.getOrElse(throw new RuntimeException("Something is wrong!")))
                    log.info(s"Persisting VS $id (${persist.get.description})...")
                  }

                  Source(vid, virtualSourceDF, keyFields)

                case VirtualFileJoinSql(id, parentSourceIds, sqlCode, keyFields, save, persist) =>
                  val parentSources = actualSourcesMapAccumulator.filterKeys(parentSourceIds.contains).values.toSeq
                  log.info(s"Processing '$id', type: 'JOIN-SQL', parent sources: ${parentSourceIds.mkString("[", ",", "]")}")
                  log.info(s"SQL: $sqlCode")

                  parentSources.foreach(src => src.df.createOrReplaceTempView(src.id))

                  parentSources.foreach(src =>
                    log.debug(s"Source '${src.id}' columns list: ${src.df.columns.toSeq.mkString("[", ",", "]")}")
                  )

                  val preDf = sparkSes.sql(sqlCode)
                  val virtualSourceDF = preDf.select(preDf.columns.map(c => col(c).as(c.toLowerCase)) : _*)

                  //persist feature
                  if (persist.isDefined) {
                    virtualSourceDF.persist(persist.getOrElse(throw new RuntimeException("Something is wrong!")))
                    log.info(s"Persisting VS $id (${persist.get.description})...")
                  }

                  Source(vid, virtualSourceDF, keyFields)

                case VirtualFileJoin(id, parentSourceIds, joiningColumns, joinType, keyFields, save, persist) =>
                  val leftParent  = parentSourceIds.head
                  val rightParent = parentSourceIds(1)
                  log.info(s"Processing '$id', type: 'JOIN', parent: L:'$leftParent', R:'$rightParent'")

                  val dfSourceLeft = actualSourcesMapAccumulator(leftParent).df
                  val dfSourceRight = actualSourcesMapAccumulator(rightParent).df

                  val colLeftRenamedLeft: Array[(String, String)] =
                    dfSourceLeft.columns
                      .filter(c => !joiningColumns.contains(c))
                      .map(colName => (colName, s"l_$colName"))
                  val colLeftRenamedRight: Array[(String, String)] =
                    dfSourceRight.columns
                      .filter(c => !joiningColumns.contains(c))
                      .map(colName => (colName, s"r_$colName"))

                  val dfLeftRenamed = colLeftRenamedLeft
                    .foldLeft(dfSourceLeft)((dfAcc, cols) => dfAcc.withColumnRenamed(cols._1, cols._2))
                  val dfRightRenamed = colLeftRenamedRight
                    .foldLeft(dfSourceRight)((dfAcc, cols) => dfAcc.withColumnRenamed(cols._1, cols._2))

                  val colLeft  = dfLeftRenamed.columns.toSeq.mkString(",")
                  val colRight = dfRightRenamed.columns.toSeq.mkString(",")

                  dfLeftRenamed.createOrReplaceTempView(leftParent)
                  dfRightRenamed.createOrReplaceTempView(rightParent)

                  log.debug(s"column left $colLeft")
                  log.debug(s"column right $colRight")

                  val preDf = dfLeftRenamed.join(dfRightRenamed, joiningColumns, joinType)
                  val virtualSourceDF = preDf.select(preDf.columns.map(c => col(c).as(c.toLowerCase)) : _*)

                  //persist feature
                  if (persist.isDefined) {
                    virtualSourceDF.persist(persist.getOrElse(throw new RuntimeException("Something is wrong!")))
                    log.info(s"Persisting VS $id (${persist.get.description})...")
                  }

                  Source(vid, virtualSourceDF, keyFields)
              }

          }
          .map(s => (s.id, s))
          .toMap
        val virtualSourcesToProcess = virtualSourcesMap -- firstLevelVirtualSources.keySet

        val processed = firstLevelVirtualSources.size

        val newActualSources = actualSourcesMapAccumulator ++ otherSources
        if (otherSources.isEmpty) {
          log.error("SOMETHING WRONG")
          throw new Exception(
            s"processed $processed : ${firstLevelVirtualSources.keySet.mkString("-")} but head only addedSize")
        }
        loop(virtualSourcesToProcess, newActualSources)
      }
    }
    loop(initialVirtualSourcesMap, initialSourceMap)
  }
}
