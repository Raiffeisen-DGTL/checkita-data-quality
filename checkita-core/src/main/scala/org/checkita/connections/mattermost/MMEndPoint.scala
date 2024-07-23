package org.checkita.connections.mattermost

import enumeratum.{Enum, EnumEntry}

import scala.collection.immutable

/**
 * List of Mattermost API end points to interact with while sending messages.
 */
sealed abstract class MMEndPoint(override val entryName: String) extends EnumEntry
object MMEndPoint extends Enum[MMEndPoint] {
  case object Users extends MMEndPoint("api/v4/users")
  case object Files extends MMEndPoint("api/v4/files")
  case object Posts extends MMEndPoint("api/v4/posts")
  case object Channels extends MMEndPoint("api/v4/channels")

  override def values: immutable.IndexedSeq[MMEndPoint] = findValues
}
