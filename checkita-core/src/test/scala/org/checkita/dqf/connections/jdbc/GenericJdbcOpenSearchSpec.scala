package org.checkita.dqf.connections.jdbc

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.types.string.NonEmptyString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.config.RefinedTypes.ID
import org.checkita.dqf.config.jobconf.Connections.GenericJdbcConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.TableSourceConfig
import org.checkita.dqf.connections.IntegrationTestTag
import org.checkita.dqf.readers.ConnectionReaders._
import org.checkita.dqf.readers.SchemaReaders.SourceSchema
import org.testcontainers.containers.wait.strategy.Wait

import java.net.{HttpURLConnection, URL}
import java.time.Duration

/**
 * Integration test for GenericJdbcConnection with OpenSearch.
 * Requires a running container runtime (Docker/Podman).
 */
class GenericJdbcOpenSearchSpec extends AnyWordSpec with Matchers with ForAllTestContainer {

  override val container: GenericContainer = GenericContainer(
    dockerImage = "opensearchproject/opensearch:2.11.0",
    exposedPorts = Seq(9200),
    env = Map(
      "discovery.type" -> "single-node",
      "plugins.security.disabled" -> "true",
      "OPENSEARCH_INITIAL_ADMIN_PASSWORD" -> "Admin_12345"
    ),
    waitStrategy = Wait.forHttp("/")
      .forPort(9200)
      .forStatusCode(200)
      .withStartupTimeout(Duration.ofSeconds(120))
  )

  implicit val schemas: Map[String, SourceSchema] = Map.empty

  private def osUrl: String = s"jdbc:opensearch://localhost:${container.mappedPort(9200)}"

  private def httpUrl: String = s"http://localhost:${container.mappedPort(9200)}"

  private def validConfig: GenericJdbcConnectionConfig = GenericJdbcConnectionConfig(
    id          = ID("opensearch_test"),
    description = None,
    url         = Refined.unsafeApply(osUrl),
    driver      = NonEmptyString.unsafeFrom("org.opensearch.jdbc.Driver"),
    username    = None,
    password    = None,
    schema      = None
  )

  private def tableSource(connId: String,
                          table: Option[String] = None,
                          query: Option[String] = None): TableSourceConfig =
    TableSourceConfig(
      id          = ID("os_source"),
      description = None,
      connection  = ID(connId),
      table       = table.map(NonEmptyString.unsafeFrom),
      query       = query.map(NonEmptyString.unsafeFrom),
      persist     = None
    )

  private def httpPut(path: String, body: String): Int = {
    val conn = new URL(s"$httpUrl$path").openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("PUT")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json")
    conn.getOutputStream.write(body.getBytes("UTF-8"))
    conn.getOutputStream.close()
    val code = conn.getResponseCode
    conn.disconnect()
    code
  }

  private def httpPost(path: String, body: String): Int = {
    val conn = new URL(s"$httpUrl$path").openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json")
    conn.getOutputStream.write(body.getBytes("UTF-8"))
    conn.getOutputStream.close()
    val code = conn.getResponseCode
    conn.disconnect()
    code
  }

  private def seedCompanies(): Unit = {
    httpPut("/test_companies", """{
      "mappings": {
        "properties": {
          "id": {"type": "integer"},
          "name": {"type": "keyword"},
          "revenue": {"type": "double"}
        }
      }
    }""")
    httpPut("/test_companies/_doc/1", """{"id": 1, "name": "Foo Corp", "revenue": 100.5}""")
    httpPut("/test_companies/_doc/2", """{"id": 2, "name": "Bar Ltd", "revenue": 200.0}""")
    httpPut("/test_companies/_doc/3", """{"id": 3, "name": "Baz Inc", "revenue": 300.75}""")
    httpPost("/test_companies/_refresh", "")
  }

  private def seedMultiTypeData(): Unit = {
    httpPut("/multi_type", """{
      "mappings": {
        "properties": {
          "id": {"type": "integer"},
          "name": {"type": "keyword"},
          "active": {"type": "boolean"}
        }
      }
    }""")
    httpPut("/multi_type/_doc/1", """{"id": 1, "name": "Alice", "active": true}""")
    httpPut("/multi_type/_doc/2", """{"id": 2, "name": "Bob", "active": false}""")
    httpPost("/multi_type/_refresh", "")
  }

  "GenericJdbcConnection with OpenSearch" must {

    "pass checkConnection against a running instance" taggedAs IntegrationTestTag in {
      val result = validConfig.read
      result.isRight shouldEqual true
    }

    "fail checkConnection when OpenSearch is unreachable" taggedAs IntegrationTestTag in {
      val badConfig = validConfig.copy(
        id  = ID("os_bad"),
        url = Refined.unsafeApply("jdbc:opensearch://localhost:19999")
      )
      badConfig.read.isLeft shouldEqual true
    }

    "connect and read data from OpenSearch index" taggedAs IntegrationTestTag in {
      seedCompanies()

      val conn = GenericJdbcConnection(validConfig)
      val df = conn.loadDataFrame(tableSource("opensearch_test", table = Some("test_companies")))
      df.count() shouldEqual 3
      df.columns.map(_.toLowerCase) should contain allOf("id", "name", "revenue")
    }

    "read data from OpenSearch using SQL query" taggedAs IntegrationTestTag in {
      val conn = GenericJdbcConnection(validConfig)
      val df = conn.loadDataFrame(tableSource(
        "opensearch_test",
        query = Some("SELECT * FROM test_companies WHERE revenue > 150")
      ))
      df.count() shouldEqual 2
    }

    "handle multiple data types (integer, keyword, boolean)" taggedAs IntegrationTestTag in {
      seedMultiTypeData()

      val conn = GenericJdbcConnection(validConfig)
      val df = conn.loadDataFrame(tableSource("opensearch_test", table = Some("multi_type")))

      df.count() shouldEqual 2
      df.columns.map(_.toLowerCase) should contain allOf("id", "name", "active")
    }
  }
}
