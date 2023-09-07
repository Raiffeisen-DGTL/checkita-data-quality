package ru.raiffeisen.checkita.checks.sql

import ru.raiffeisen.checkita.checks.{CheckResult, CheckUtil}
import ru.raiffeisen.checkita.sources.DatabaseConfig

import java.sql.Connection
import scala.util.Try


/**
 * Performs check based on sql query
 *
 * @param id check id
 * @param description check description
 * @param subType type of check ("EQ_ZERO","NOT_EQ_ZERO",...)
 * @param source database name
 * @param sourceConfig database configuration
 * @param query check query
 */
case class SQLCheck(
                     id: String,
                     description: String,
                     subType: String,
                     source: String,
                     sourceConfig: DatabaseConfig,
                     query: String
                   ) {

  def executeCheck(connection: Connection): CheckResult = {
    val transformations = SQLCheckProcessor.getTransformations(subType)

    val statement = connection.createStatement()
    statement.setFetchSize(1000)

    val queryResult = statement.executeQuery(query)

    val result = transformations._1(queryResult)
    statement.close()

    val status = CheckUtil.tryToStatus(Try(result), transformations._2)

    val cr =
      CheckResult(
        this.id,
        subType,
        this.description,
        this.source,
        "",
        Some(""),
        0.0,
        None,
        None,
        status.stringValue,
        this.query
      )

    cr
  }
}

