package ru.raiffeisen.checkita.checks

import java.sql.Timestamp

case class CheckResultFormatted(jobId: String,
                                checkId: String,
                                checkName: String,
                                description: String,
                                sourceId: String,
                                baseMetric: String,
                                comparedMetric: String,
                                comparedThreshold: Double,
                                lowerBound: String,
                                upperBound: String,
                                status: String,
                                message: String,
                                referenceDate: Timestamp,
                                executionDate: Timestamp)

case class LoadCheckResultFormatted(jobId: String,
                                    checkId: String,
                                    checkName: String,
                                    sourceId: String,
                                    expected: String,
                                    status: String,
                                    message: String,
                                    referenceDate: Timestamp,
                                    executionDate: Timestamp)
