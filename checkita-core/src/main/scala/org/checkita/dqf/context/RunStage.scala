package org.checkita.dqf.context

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
 * Application execution stages. Used in log messages.
 */
sealed abstract class RunStage(override val entryName: String) extends EnumEntry
object RunStage extends Enum[RunStage] {
  case object ApplicationSetup extends RunStage("[APPLICATION SETUP]")
  case object CheckpointStage extends RunStage("[READING CHECKPOINT]")
  case object EstablishConnections extends RunStage("[ESTABLISH CONNECTIONS]")
  case object ReadSchemas extends RunStage("[READING SCHEMAS]")
  case object ReadSources extends RunStage("[READING REGULAR SOURCES]")
  case object ReadVirtualSources extends RunStage("[READING VIRTUAL SOURCES]")
  case object MetricCalculation extends RunStage("[METRICS CALCULATION]")
  case object PerformLoadChecks extends RunStage("[PERFORMING LOAD CHECKS]")
  case object PerformChecks extends RunStage("[PERFORMING CHECKS]")
  case object SaveResults extends RunStage("[RESULTS SAVING]")
  case object ProcessTargets extends RunStage("[SENDING TARGETS]")
  case object CheckProcessorBuffer extends RunStage("[CHECKING PROCESSOR BUFFER]")
  override def values: immutable.IndexedSeq[RunStage] = findValues
}