package ru.raiffeisen.checkita.storage

import org.flywaydb.core.Flyway
import ru.raiffeisen.checkita.storage.Connections.DqStorageJdbcConnection

import scala.jdk.CollectionConverters._

class MigrationRunner(ds: DqStorageJdbcConnection) {
  
  private val scriptsLocation: String = s"db/specific/${ds.getType.toString.toLowerCase}"
  private val variables: java.util.Map[String, String] = ds.getSchema match {
    case Some(schema) => Map("defaultSchema" -> schema).asJava
    case None => Map.empty[String, String].asJava
  }
  
  private val flyway = {
    val fw = Flyway.configure()
      .locations(scriptsLocation)
      .placeholders(variables)
      .dataSource(ds.getDataSource)
      .baselineOnMigrate(true)
      .baselineVersion("0.0")
      
    if (ds.getSchema.nonEmpty) fw.defaultSchema(ds.getSchema.get)
    fw.load()
  }

  def run(): Unit = flyway.migrate()
  
}
