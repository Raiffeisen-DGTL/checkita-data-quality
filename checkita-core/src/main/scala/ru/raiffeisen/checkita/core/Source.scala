package ru.raiffeisen.checkita.core

import org.apache.spark.sql.DataFrame

/**
 * Data Quality Source definition
 *
 * @param id        Source ID
 * @param df        Spark dataframe with source data
 * @param keyFields Key field (columns) of this source: uniquely define data row
 * @param parents   Sequence of parent sources IDs from which this source is build (applies to virtual sources)
 */
case class Source(
                   id: String,
                   df: DataFrame,
                   keyFields: Seq[String] = Seq.empty,
                   parents: Seq[String] = Seq.empty
                 )
