package org.checkita.dqf.connections.jdbc

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import org.apache.logging.log4j.Level
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.config.IO.readJobConfig
import org.checkita.dqf.connections.IntegrationTestTag
import org.checkita.dqf.context.DQContext
import org.testcontainers.containers.wait.strategy.Wait

import java.io.{File, PrintWriter}
import java.nio.file.Files
import java.sql.DriverManager
import java.time.Duration

/**
 * End-to-end integration test: runs a full Checkita DQ pipeline
 * (connection → source → metrics → checks) against a real PostgreSQL database.
 * Requires a running container runtime (Docker/Podman).
 */
class GenericJdbcE2ESpec extends AnyWordSpec with Matchers with ForAllTestContainer {

  override val container: GenericContainer = GenericContainer(
    dockerImage = "postgres:15",
    exposedPorts = Seq(5432),
    env = Map(
      "POSTGRES_DB" -> "dq_e2e",
      "POSTGRES_USER" -> "dq_user",
      "POSTGRES_PASSWORD" -> "dq_pass"
    ),
    waitStrategy = Wait.forLogMessage(".*database system is ready to accept connections.*", 2)
      .withStartupTimeout(Duration.ofSeconds(60))
  )

  private def pgPort: Int = container.mappedPort(5432)

  private def seedData(): Unit = {
    val conn = DriverManager.getConnection(
      s"jdbc:postgresql://localhost:$pgPort/dq_e2e", "dq_user", "dq_pass"
    )
    val stmt = conn.createStatement()
    stmt.execute(
      """CREATE TABLE IF NOT EXISTS sales (
        |  id SERIAL PRIMARY KEY,
        |  product VARCHAR(50),
        |  region VARCHAR(30),
        |  amount NUMERIC(10,2),
        |  category VARCHAR(30)
        |)""".stripMargin
    )
    stmt.execute("DELETE FROM sales")
    stmt.execute(
      """INSERT INTO sales (product, region, amount, category) VALUES
        |  ('Widget A', 'North', 150.00, 'Electronics'),
        |  ('Widget B', 'South', 230.50, 'Electronics'),
        |  ('Gadget C', 'North', NULL,    'Gadgets'),
        |  ('Widget A', 'East',  89.99,  'Electronics'),
        |  ('Gadget D', 'South', 445.00, NULL),
        |  ('Widget B', 'West',  175.25, 'Electronics'),
        |  ('Gadget C', 'North', 320.00, 'Gadgets'),
        |  ('Widget A', 'South', 210.00, 'Electronics'),
        |  ('Gadget D', 'East',  NULL,    'Gadgets'),
        |  ('Widget B', 'North', 190.75, 'Electronics')""".stripMargin
    )
    stmt.close()
    conn.close()
  }

  private def jobConfigString: String =
    s"""
    jobConfig: {
      jobId: "e2e_test_job"
      connections: {
        postgres: [{
          id: "pg_e2e"
          url: "localhost:$pgPort/dq_e2e"
          username: "dq_user"
          password: "dq_pass"
          schema: "public"
        }]
      }
      sources: {
        table: [{
          id: "sales"
          connection: "pg_e2e"
          table: "sales"
          keyFields: ["id"]
        }]
      }
      metrics: {
        regular: {
          rowCount: [
            {id: "sales_row_cnt", source: "sales"}
          ]
          nullValues: [
            {id: "sales_amount_nulls", source: "sales", columns: ["amount"]}
            {id: "sales_category_nulls", source: "sales", columns: ["category"]}
          ]
          completeness: [
            {id: "sales_amount_completeness", source: "sales", columns: ["amount"]}
          ]
          distinctValues: [
            {id: "sales_distinct_products", source: "sales", columns: ["product"]}
            {id: "sales_distinct_regions", source: "sales", columns: ["region"]}
          ]
        }
      }
      checks: {
        snapshot: {
          greaterThan: [
            {id: "has_rows", metric: "sales_row_cnt", threshold: 0}
            {id: "completeness_above_70pct", metric: "sales_amount_completeness", threshold: 0.7}
          ]
          lessThan: [
            {id: "amount_nulls_below_5", metric: "sales_amount_nulls", threshold: 5}
          ]
          equalTo: [
            {id: "exactly_4_products", metric: "sales_distinct_products", threshold: 4}
            {id: "exactly_4_regions", metric: "sales_distinct_regions", threshold: 4}
          ]
        }
      }
    }
    """

  private val appConfigString: String =
    """
    appConfig: {
      applicationName: "E2E Test"
      dateTimeOptions: {
        timeZone: "UTC"
        referenceDateFormat: "yyyy-MM-dd"
        executionDateFormat: "yyyy-MM-dd HH:mm:ss"
      }
      enablers: {
        allowSqlQueries: true
      }
      defaultSparkOptions: [
        "spark.driver.host=localhost"
        "spark.sql.shuffle.partitions=4"
      ]
    }
    """

  private def writeToTempFile(content: String, prefix: String): String = {
    val file = Files.createTempFile(prefix, ".conf").toFile
    val pw = new PrintWriter(file)
    pw.write(content)
    pw.close()
    file.getAbsolutePath
  }

  "Full Checkita DQ pipeline with PostgreSQL" must {

    "calculate metrics and run checks correctly" taggedAs IntegrationTestTag in {
      seedData()

      val appConfigPath = writeToTempFile(appConfigString, "app_config_")
      val jobConfigPath = writeToTempFile(jobConfigString, "job_config_")

      // Build DQ context
      val dqContext = DQContext.build(
        appConfigPath, Some("2024-01-05"),
        isLocal = true, isShared = true,
        logLvl = Level.WARN
      )
      dqContext.left.foreach(errors => fail(s"DQContext build failed: ${errors.mkString("\n")}"))
      val ctx = dqContext.toOption.get

      // Build job from config file
      val jobConfig = readJobConfig[File](new File(jobConfigPath)).toOption.get
      val job = ctx.buildBatchJob(jobConfig)
      job.left.foreach(errors => fail(s"Job build failed: ${errors.mkString("\n")}"))
      job.isRight shouldEqual true

      // Run job
      val resultSet = job.toOption.get.run
      resultSet.isRight shouldEqual true
      val results = resultSet.toOption.get

      // Verify metrics
      val metrics = results.regularMetrics
      val rowCnt = metrics.find(_.metricId == "sales_row_cnt").get
      rowCnt.result shouldEqual 10.0

      val amountNulls = metrics.find(_.metricId == "sales_amount_nulls").get
      amountNulls.result shouldEqual 2.0

      val categoryNulls = metrics.find(_.metricId == "sales_category_nulls").get
      categoryNulls.result shouldEqual 1.0

      val completeness = metrics.find(_.metricId == "sales_amount_completeness").get
      completeness.result shouldEqual 0.8

      val distinctProducts = metrics.find(_.metricId == "sales_distinct_products").get
      distinctProducts.result shouldEqual 4.0

      val distinctRegions = metrics.find(_.metricId == "sales_distinct_regions").get
      distinctRegions.result shouldEqual 4.0

      // Verify checks
      val checks = results.checks
      checks should have size 5

      val hasRows = checks.find(_.checkId == "has_rows").get
      hasRows.status shouldEqual "Success"

      val completenessCheck = checks.find(_.checkId == "completeness_above_70pct").get
      completenessCheck.status shouldEqual "Success"

      val nullsCheck = checks.find(_.checkId == "amount_nulls_below_5").get
      nullsCheck.status shouldEqual "Success"

      val productsCheck = checks.find(_.checkId == "exactly_4_products").get
      productsCheck.status shouldEqual "Success"

      val regionsCheck = checks.find(_.checkId == "exactly_4_regions").get
      regionsCheck.status shouldEqual "Success"

      // No failure tolerance violations
      results.failureToleranceViolationChecks shouldBe empty
    }
  }
}
