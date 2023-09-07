package ru.raiffeisen.checkita.metrics

import java.sql.Timestamp

case class ColumnMetricResultFormatted(jobId: String,
                                       metricId: String,
                                       metricName: String,
                                       description: String,
                                       sourceId: String,
                                       columnNames: String,
                                       params: String,
                                       result: Double,
                                       additionalResult: String,
                                       referenceDate: Timestamp,
                                       executionDate: Timestamp)

case class FileMetricResultFormatted(jobId: String,
                                     metricId: String,
                                     metricName: String,
                                     description: String,
                                     sourceId: String,
                                     result: Double,
                                     additionalResult: String,
                                     referenceDate: Timestamp,
                                     executionDate: Timestamp)

case class ComposedMetricResultFormatted(jobId: String,
                                         metricId: String,
                                         metricName: String,
                                         description: String,
                                         sourceId: String,
                                         formula: String,
                                         result: Double,
                                         additionalResult: String,
                                         referenceDate: Timestamp,
                                         executionDate: Timestamp)
