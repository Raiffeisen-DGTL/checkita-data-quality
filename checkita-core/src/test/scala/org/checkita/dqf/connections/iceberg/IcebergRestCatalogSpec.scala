package org.checkita.dqf.connections.iceberg

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.types.string.NonEmptyString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.config.RefinedTypes.ID
import org.checkita.dqf.config.jobconf.Connections.IcebergConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.IcebergSourceConfig
import org.checkita.dqf.connections.IntegrationTestTag
import org.checkita.dqf.readers.ConnectionReaders._
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.testcontainers.containers.wait.strategy.Wait

import java.nio.file.Files
import java.time.Duration

/**
 * Integration test for IcebergConnection with REST catalog.
 * Requires a running container runtime (Docker/Podman).
 */
class IcebergRestCatalogSpec extends AnyWordSpec with Matchers with ForAllTestContainer {

  private val warehousePath = Files.createTempDirectory("iceberg_rest_warehouse").toAbsolutePath.toString
  private val catalogName = "rest_it_catalog"

  override val container: GenericContainer = GenericContainer(
    dockerImage = "tabulario/iceberg-rest:0.13.0",
    exposedPorts = Seq(8181),
    env = Map(
      "CATALOG_WAREHOUSE" -> "file:///tmp/warehouse",
      "CATALOG_IO__IMPL" -> "org.apache.iceberg.io.ResolvingFileIO"
    ),
    waitStrategy = Wait.forHttp("/v1/config")
      .forPort(8181)
      .forStatusCode(200)
      .withStartupTimeout(Duration.ofSeconds(60))
  )

  implicit val schemas: Map[String, SourceSchema] = Map.empty

  private def restUri: String = s"http://localhost:${container.mappedPort(8181)}"

  private def validConfig: IcebergConnectionConfig = IcebergConnectionConfig(
    id          = ID("iceberg_rest"),
    description = None,
    catalogName = NonEmptyString.unsafeFrom(catalogName),
    catalogType = NonEmptyString.unsafeFrom("rest"),
    warehouse   = Some(Refined.unsafeApply(warehousePath)),
    catalogUri  = Some(Refined.unsafeApply(restUri))
  )

  private def icebergSource(table: String, database: Option[String] = None): IcebergSourceConfig =
    IcebergSourceConfig(
      id          = ID("it_source"),
      description = None,
      connection  = ID("iceberg_rest"),
      table       = NonEmptyString.unsafeFrom(table),
      database    = database.map(NonEmptyString.unsafeFrom),
      persist     = None
    )

  private def setupCatalog(): Unit = {
    spark.conf.set(s"spark.sql.catalog.$catalogName", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(s"spark.sql.catalog.$catalogName.type", "rest")
    spark.conf.set(s"spark.sql.catalog.$catalogName.uri", restUri)
    spark.conf.set(s"spark.sql.catalog.$catalogName.warehouse", warehousePath)
  }

  "IcebergConnection with REST catalog" must {

    "pass checkConnection when Iceberg runtime is on classpath" taggedAs IntegrationTestTag in {
      val result = validConfig.read
      result.isRight shouldEqual true
    }

    "connect to REST catalog and create/read tables" taggedAs IntegrationTestTag in {
      setupCatalog()

      spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalogName.test_ns")
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $catalogName.test_ns.events (
           |  event_id INT,
           |  event_type STRING,
           |  payload STRING
           |) USING iceberg""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO $catalogName.test_ns.events VALUES
           |(1, 'click', '{"page": "/home"}'),
           |(2, 'view', '{"page": "/about"}'),
           |(3, 'click', '{"page": "/products"}')""".stripMargin
      )

      val conn = IcebergConnection(validConfig)
      val df = conn.loadDataFrame(icebergSource("events", Some("test_ns")))
      df.count() shouldEqual 3
      df.columns.map(_.toLowerCase) should contain allOf("event_id", "event_type", "payload")
    }

    "read from default namespace" taggedAs IntegrationTestTag in {
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $catalogName.default.metrics (
           |  metric_name STRING,
           |  metric_value DOUBLE
           |) USING iceberg""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO $catalogName.default.metrics VALUES
           |('cpu_usage', 75.5),
           |('mem_usage', 82.3)""".stripMargin
      )

      val conn = IcebergConnection(validConfig)
      val df = conn.loadDataFrame(icebergSource("metrics"))
      df.count() shouldEqual 2
    }

    "handle various data types including timestamps and decimals" taggedAs IntegrationTestTag in {
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $catalogName.test_ns.typed_table (
           |  id INT,
           |  name STRING,
           |  active BOOLEAN,
           |  score DOUBLE,
           |  amount DECIMAL(10,2),
           |  created_at TIMESTAMP,
           |  event_date DATE
           |) USING iceberg""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO $catalogName.test_ns.typed_table VALUES
           |(1, 'Alice', true, 95.5, 1234.56, TIMESTAMP '2024-01-15 10:30:00', DATE '2024-01-15'),
           |(2, 'Bob', false, 42.0, 789.00, TIMESTAMP '2024-06-20 14:00:00', DATE '2024-06-20')""".stripMargin
      )

      val conn = IcebergConnection(validConfig)
      val df = conn.loadDataFrame(icebergSource("typed_table", Some("test_ns")))

      df.count() shouldEqual 2
      df.columns.map(_.toLowerCase) should contain allOf(
        "id", "name", "active", "score", "amount", "created_at", "event_date"
      )

      val row1 = df.filter("id = 1").collect().head
      row1.getAs[Boolean]("active") shouldEqual true
    }

    "handle NULL values correctly" taggedAs IntegrationTestTag in {
      spark.sql(
        s"""CREATE TABLE IF NOT EXISTS $catalogName.test_ns.nullable_table (
           |  id INT,
           |  optional_field STRING
           |) USING iceberg""".stripMargin
      )
      spark.sql(
        s"""INSERT INTO $catalogName.test_ns.nullable_table VALUES
           |(1, 'present'),
           |(2, NULL)""".stripMargin
      )

      val conn = IcebergConnection(validConfig)
      val df = conn.loadDataFrame(icebergSource("nullable_table", Some("test_ns")))
      df.count() shouldEqual 2

      val nullRow = df.filter("id = 2").select("optional_field").collect().head
      nullRow.isNullAt(0) shouldEqual true

      val nonNullRow = df.filter("id = 1").select("optional_field").collect().head
      nonNullRow.getString(0) shouldEqual "present"
    }

    "fail to read a non-existent table" taggedAs IntegrationTestTag in {
      val conn = IcebergConnection(validConfig)
      an[Exception] should be thrownBy {
        conn.loadDataFrame(icebergSource("nonexistent_table_xyz", Some("test_ns")))
      }
    }
  }
}
