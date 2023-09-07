package ru.raiffeisen.checkita.sources

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import ru.raiffeisen.checkita.exceptions.IllegalParameterException
import ru.raiffeisen.checkita.utils.io.dbreaders.{ORCLReader, PostgresReader, SQLiteReader, TableReader}
import ru.raiffeisen.checkita.utils.makeTableName

import java.sql.Connection
import scala.util.Try

case class DatabaseConfig(
                           id: String,
                           subtype: String,
                           host: String,
                           port: Option[String] = None,
                           service: Option[String] = None,
                           user: Option[String] = None,
                           password: Option[String] = None,
                           schema: Option[String] = None
                         ) {

  // Constructor for
  def this(config: Config) = {
    this(
      Try(config.getString("id")).getOrElse(""),
      config.getString("subtype"),
      config.getString("host"),
      Try(config.getString("port")).toOption,
      Try(config.getString("service")).toOption,
      Try(config.getString("user")).toOption,
      Try(config.getString("password")).toOption,
      Try(config.getString("schema")).toOption
    )
  }

  private val dbReader: TableReader = subtype.toUpperCase() match {
    case "ORACLE"     => ORCLReader(this)
    case "SQLITE"     => SQLiteReader(this)
    case "POSTGRESQL" => PostgresReader(this)
    case x            => throw IllegalParameterException(x)
  }

  def getConnection: Connection = dbReader.getConnection
  def getUrl: String = dbReader.getUrl
  // the trick here is that table credentials can be different from database one,
  // so that function allow you to connect to the database with multiple credentials
  // without specification of multiple databases
  def loadData(table: String,
               user: Option[String] = this.user,
               password: Option[String] = this.password)(
                implicit sparkSess: SparkSession): DataFrame =
    dbReader.loadData(makeTableName(schema, table), user, password)
}
