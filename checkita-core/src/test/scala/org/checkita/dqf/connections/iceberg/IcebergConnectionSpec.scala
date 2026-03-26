package org.checkita.dqf.connections.iceberg

import eu.timepit.refined.api.Refined
import eu.timepit.refined.types.string.NonEmptyString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.config.RefinedTypes.ID
import org.checkita.dqf.config.jobconf.Connections.IcebergConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.IcebergSourceConfig
import org.checkita.dqf.readers.ConnectionReaders._
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

import java.nio.file.Files

class IcebergConnectionSpec extends AnyWordSpec with Matchers {

  private val warehousePath = Files.createTempDirectory("iceberg_test_warehouse").toAbsolutePath.toString

  private val validConfig = IcebergConnectionConfig(
    id          = ID("iceberg_test"),
    description = None,
    catalogName = NonEmptyString.unsafeFrom("test_catalog"),
    catalogType = NonEmptyString.unsafeFrom("hadoop"),
    warehouse   = Some(Refined.unsafeApply(warehousePath)),
    catalogUri  = None
  )

  implicit val schemas: Map[String, SourceSchema] = Map.empty

  private def icebergSource(connId: String,
                            table: String,
                            database: Option[String] = None): IcebergSourceConfig =
    IcebergSourceConfig(
      id          = ID("test_iceberg_source"),
      description = None,
      connection  = ID(connId),
      table       = NonEmptyString.unsafeFrom(table),
      database    = database.map(NonEmptyString.unsafeFrom),
      persist     = None
    )

  "IcebergConnection" must {

    "be instantiated with correct id from config" in {
      val conn = IcebergConnection(validConfig)
      conn.id shouldEqual "iceberg_test"
    }

    "pass connection check when Iceberg runtime is on classpath" in {
      val result = validConfig.read
      result.isRight shouldEqual true
    }

    "create and read an Iceberg table using Hadoop catalog" in {
      val conn = IcebergConnection(validConfig)

      // Create table and insert data via Spark SQL
      val catalogName = "test_catalog"
      spark.conf.set(s"spark.sql.catalog.$catalogName", "org.apache.iceberg.spark.SparkCatalog")
      spark.conf.set(s"spark.sql.catalog.$catalogName.type", "hadoop")
      spark.conf.set(s"spark.sql.catalog.$catalogName.warehouse", warehousePath)

      spark.sql(s"CREATE DATABASE IF NOT EXISTS $catalogName.test_db")
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $catalogName.test_db.companies (
           |  id INT,
           |  name STRING,
           |  revenue DOUBLE
           |) USING iceberg""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO $catalogName.test_db.companies VALUES
           |(1, 'Foo Corp', 100.5),
           |(2, 'Bar Ltd', 200.0),
           |(3, 'Baz Inc', 300.75)""".stripMargin
      )

      val df = conn.loadDataFrame(icebergSource("iceberg_test", "companies", Some("test_db")))
      df.count() shouldEqual 3
      df.columns.map(_.toLowerCase) should contain allOf("id", "name", "revenue")
    }

    "use default database when database is not specified" in {
      val conn = IcebergConnection(validConfig)
      val catalogName = "test_catalog"

      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $catalogName.default.default_table (
           |  id INT,
           |  value STRING
           |) USING iceberg""".stripMargin
      )
      spark.sql(s"INSERT INTO $catalogName.default.default_table VALUES (1, 'test')")

      val df = conn.loadDataFrame(icebergSource("iceberg_test", "default_table"))
      df.count() shouldEqual 1
    }

    "handle NULL values correctly" in {
      val conn = IcebergConnection(validConfig)
      val catalogName = "test_catalog"

      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $catalogName.test_db.nullable_table (
           |  id INT,
           |  name STRING
           |) USING iceberg""".stripMargin
      )
      spark.sql(s"INSERT INTO $catalogName.test_db.nullable_table VALUES (1, 'hello'), (2, NULL)")

      val df = conn.loadDataFrame(icebergSource("iceberg_test", "nullable_table", Some("test_db")))
      df.count() shouldEqual 2

      val nullRow = df.filter("id = 2").select("name").collect().head
      nullRow.isNullAt(0) shouldEqual true
    }
  }
}
