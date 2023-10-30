package ru.raiffeisen.checkita.storage

import com.zaxxer.hikari.HikariDataSource
import ru.raiffeisen.checkita.config.appconf.StorageConfig
import ru.raiffeisen.checkita.config.Enums.DQStorageType

import java.io.{PrintWriter, StringWriter}
import java.sql.Connection
import javax.sql.DataSource

object Connections {

  sealed abstract class DqStorageConnection {
    protected val config: StorageConfig
    def getType: DQStorageType = config.dbType
  }
  
  object DqStorageConnection{
    def apply(config: StorageConfig): DqStorageConnection = config.dbType match {
      case DQStorageType.File => new DqStorageFileConnection(config)
      case DQStorageType.Hive => new DqStorageHiveConnection(config)
      case _ => new DqStorageJdbcConnection(config)
    }
  }
  
  class DqStorageFileConnection(val config: StorageConfig) extends DqStorageConnection {
    def getPath: String = config.url.value
  }

  class DqStorageHiveConnection(val config: StorageConfig) extends DqStorageConnection {
    def getSchema: String = config.schema.map(_.value).getOrElse(throw new IllegalArgumentException(
      "Cannot create connection to Hive storage: configuration does not contain schema name."
    ))
  }
  
  class DqStorageJdbcConnection(val config: StorageConfig) extends DqStorageConnection {
    
    val writer: StringWriter = new StringWriter()
    
    private val ds: HikariDataSource = {
      val hikariDs = new HikariDataSource()
      
      config.dbType match {
        case DQStorageType.Postgres =>
          hikariDs.setJdbcUrl("jdbc:postgresql://" + config.url.value)
        case DQStorageType.Oracle =>
          hikariDs.setJdbcUrl("jdbc:oracle:thin:@" + config.url.value)
        case DQStorageType.MySql =>
          hikariDs.setJdbcUrl("jdbc:mysql://" + config.url.value + "?sessionVariables=sql_mode=ANSI_QUOTES")
          
          hikariDs.addDataSourceProperty("cachePrepStmts", true)
          hikariDs.addDataSourceProperty("prepStmtCacheSize", 250)
          hikariDs.addDataSourceProperty("prepStmtCacheSqlLimit", 2048)
          hikariDs.addDataSourceProperty("useServerPrepStmts", true)
          hikariDs.addDataSourceProperty("useLocalSessionState", true)
          hikariDs.addDataSourceProperty("rewriteBatchedStatements", true)
          hikariDs.addDataSourceProperty("cacheResultSetMetadata", true)
          hikariDs.addDataSourceProperty("cacheServerConfiguration", true)
          hikariDs.addDataSourceProperty("elideSetAutoCommits", true)
          hikariDs.addDataSourceProperty("maintainTimeStats", true)
          
        case DQStorageType.MSSql =>
          hikariDs.setJdbcUrl("jdbc:jtds:sqlserver://" + config.url.value)
          hikariDs.setDriverClassName("net.sourceforge.jtds.jdbc.Driver")
          hikariDs.setConnectionTestQuery("SELECT GETDATE()")
        case DQStorageType.H2 =>
          hikariDs.setJdbcUrl("jdbc:h2:tcp://" + config.url.value)
        case DQStorageType.SQLite =>
          hikariDs.setJdbcUrl("jdbc:sqlite:" + config.url.value)
        case otherType => throw new IllegalArgumentException(
          "Cannot create jdbc connection to storage due to non-jdbc " +
            s"dbType of '${otherType.toString}' is specified in the configuration."
        )
      }
      
      config.username.map(_.value).foreach(hikariDs.setUsername)
      config.password.map(_.value).foreach(hikariDs.setPassword)
      
      hikariDs.setAutoCommit(true)
      hikariDs.setLogWriter(new PrintWriter(writer))
      
      hikariDs
    }
    
    def getMaxConnections: Int = ds.getMaximumPoolSize
    def getDataSource: DataSource = ds
    def getConnection: Connection = ds.getConnection
    def getSchema: Option[String] = config.dbType match {
      case DQStorageType.Oracle => config.schema.map(_.value) orElse Option(ds.getUsername)
      case _ => config.schema.map(_.value) orElse Option(ds.getSchema)
    }
  }
  
}
