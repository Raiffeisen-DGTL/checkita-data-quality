package ru.raiffeisen.checkita.core

import org.apache.spark.sql.DataFrame
import ru.raiffeisen.checkita.core.streaming.Checkpoints.Checkpoint

/**
 * Data Quality Source definition
 *
 * @note Source can hold both static and streaming dataframes and, therefore, both batch source readers
 *       and stream source readers return same source definition.
 * @param id         Source ID
 * @param df         Spark dataframe with source data
 * @param keyFields  Key fields (columns) of this source: uniquely define data row
 * @param parents    Sequence of parent sources IDs from which this source is build (applies to virtual sources)
 * @param checkpoint Initial source checkpoint. Applicable only for streaming sources. 
 */
case class Source(
                   id: String,
                   df: DataFrame,
                   keyFields: Seq[String] = Seq.empty,
                   parents: Seq[String] = Seq.empty,
                   checkpoint: Option[Checkpoint] = None
                 ) {
  val isStreaming: Boolean = df.isStreaming
}
object Source {

  /**
   * Creates Source instance with additional validation of its fields.
   *
   * @param id            Source ID
   * @param df            Spark dataframe with source data
   * @param keyFields     Key fields (columns) of this source: uniquely define data row
   * @param parents       Sequence of parent sources IDs from which this source is build (applies to virtual sources)
   * @param checkpoint    Initial source checkpoint. Applicable only for streaming sources.
   * @param caseSensitive Implicit flag defining whether column names are case sensitive or not.
   * @return Source instance
   */
  def validated(id: String,
                df: DataFrame,
                keyFields: Seq[String] = Seq.empty,
                parents: Seq[String] = Seq.empty,
                checkpoint: Option[Checkpoint] = None)(implicit caseSensitive: Boolean): Source = {

    val kf = if (caseSensitive) keyFields else keyFields.map(_.toLowerCase)
    val cols = if (caseSensitive) df.columns else df.columns.map(_.toLowerCase)
    val missedKf = kf.filterNot(cols.contains)
    require(
      missedKf.isEmpty,
      s"Some of key fields were not found for source '$id'. " +
        "Following keyFields are not found within source columns: " +
        missedKf.mkString("[`", "`, `", "`]")
    )
    Source(id, df, kf, parents, checkpoint)
  }
}