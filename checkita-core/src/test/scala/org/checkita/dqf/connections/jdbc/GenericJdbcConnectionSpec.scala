package org.checkita.dqf.connections.jdbc

import eu.timepit.refined.api.Refined
import eu.timepit.refined.types.string.NonEmptyString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.config.RefinedTypes.ID
import org.checkita.dqf.config.jobconf.Connections.GenericJdbcConnectionConfig
import org.checkita.dqf.config.jobconf.Sources.TableSourceConfig
import org.checkita.dqf.readers.ConnectionReaders._
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

import java.sql.DriverManager

class GenericJdbcConnectionSpec extends AnyWordSpec with Matchers {

  private val h2Driver = "org.h2.Driver"
  private val h2Url    = "jdbc:h2:mem:generic_jdbc_test"

  // Keep an open connection so the in-memory H2 DB stays alive for all tests
  private val setupConn = {
    Class.forName(h2Driver)
    val c = DriverManager.getConnection(h2Url)
    val stmt = c.createStatement()
    stmt.execute(
      """CREATE TABLE IF NOT EXISTS test_companies (
        |  id INT,
        |  name VARCHAR(100),
        |  revenue DOUBLE,
        |  is_active BOOLEAN,
        |  founded DATE,
        |  created_at TIMESTAMP,
        |  rating DECIMAL(5,2),
        |  description VARCHAR(200)
        |)""".stripMargin
    )
    stmt.execute(
      "INSERT INTO test_companies SELECT 1, 'Foo Corp', 100.5, TRUE, '2020-01-15', '2020-01-15 10:30:00', 4.50, 'First company' " +
        "WHERE NOT EXISTS (SELECT 1 FROM test_companies WHERE id = 1)"
    )
    stmt.execute(
      "INSERT INTO test_companies SELECT 2, 'Bar Ltd', 200.0, FALSE, '2019-06-20', '2019-06-20 14:00:00', 3.75, NULL " +
        "WHERE NOT EXISTS (SELECT 1 FROM test_companies WHERE id = 2)"
    )
    stmt.execute(
      "INSERT INTO test_companies SELECT 3, 'Baz Inc', 300.75, TRUE, '2021-03-10', '2021-03-10 09:15:00', 4.95, 'Third company' " +
        "WHERE NOT EXISTS (SELECT 1 FROM test_companies WHERE id = 3)"
    )
    c
  }

  private val validConfig = GenericJdbcConnectionConfig(
    id          = ID("h2_generic"),
    description = None,
    url         = Refined.unsafeApply(h2Url),
    driver      = NonEmptyString.unsafeFrom(h2Driver),
    username    = None,
    password    = None,
    schema      = None
  )

  private val h2AuthUrl = "jdbc:h2:mem:generic_jdbc_auth_test"
  private val authSetupConn = {
    val c = DriverManager.getConnection(h2AuthUrl, "testuser", "testpass")
    val stmt = c.createStatement()
    stmt.execute("CREATE TABLE IF NOT EXISTS auth_table (id INT, name VARCHAR(50))")
    stmt.execute("INSERT INTO auth_table SELECT 1, 'auth_test' WHERE NOT EXISTS (SELECT 1 FROM auth_table WHERE id = 1)")
    c
  }

  private val configWithAuth = GenericJdbcConnectionConfig(
    id          = ID("h2_with_auth"),
    description = None,
    url         = Refined.unsafeApply(h2AuthUrl),
    driver      = NonEmptyString.unsafeFrom(h2Driver),
    username    = Some(NonEmptyString.unsafeFrom("testuser")),
    password    = Some(NonEmptyString.unsafeFrom("testpass")),
    schema      = None
  )

  implicit val schemas: Map[String, SourceSchema] = Map.empty

  private def tableSource(connId: String,
                          table: Option[String] = None,
                          query: Option[String] = None): TableSourceConfig =
    TableSourceConfig(
      id          = ID("test_source"),
      description = None,
      connection  = ID(connId),
      table       = table.map(NonEmptyString.unsafeFrom),
      query       = query.map(NonEmptyString.unsafeFrom),
      persist     = None
    )

  "GenericJdbcConnection" must {

    "be instantiated with correct id from config" in {
      val conn = GenericJdbcConnection(validConfig)
      conn.id shouldEqual "h2_generic"
    }

    "pass connection check when database is available" in {
      val result = validConfig.read
      result.isRight shouldEqual true
    }

    "pass connection check with username and password" in {
      val result = configWithAuth.read
      result.isRight shouldEqual true
    }

    "fail connection check when driver class is not found" in {
      val badDriver = validConfig.copy(
        id     = ID("bad_driver"),
        driver = NonEmptyString.unsafeFrom("com.nonexistent.Driver")
      )
      badDriver.read.isLeft shouldEqual true
    }

    "fail connection check when URL is not handled by any registered driver" in {
      val badConfig = validConfig.copy(
        id  = ID("bad_conn"),
        url = Refined.unsafeApply("jdbc:unknowndb://nonexistent-host:9999/db")
      )
      badConfig.read.isLeft shouldEqual true
    }

    "load data from a table" in {
      val conn = GenericJdbcConnection(validConfig)
      val df = conn.loadDataFrame(tableSource("h2_generic", table = Some("test_companies")))
      df.count() shouldEqual 3
      df.columns.map(_.toLowerCase) should contain allOf("id", "name", "revenue")
    }

    "load data using an arbitrary SQL query" in {
      val conn = GenericJdbcConnection(validConfig)
      val df = conn.loadDataFrame(tableSource(
        "h2_generic",
        query = Some("SELECT * FROM test_companies WHERE revenue > 150")
      ))
      df.count() shouldEqual 2
    }

    "throw an exception when both table and query are provided" in {
      val conn = GenericJdbcConnection(validConfig)
      an[IllegalArgumentException] should be thrownBy {
        conn.loadDataFrame(tableSource(
          "h2_generic",
          table = Some("test_companies"),
          query = Some("SELECT 1")
        ))
      }
    }

    "throw an exception when neither table nor query is provided" in {
      val conn = GenericJdbcConnection(validConfig)
      an[IllegalArgumentException] should be thrownBy {
        conn.loadDataFrame(tableSource("h2_generic"))
      }
    }

    "apply schema prefix to table name when schema is configured" in {
      val conn = DriverManager.getConnection(h2Url)
      conn.createStatement().execute("CREATE SCHEMA IF NOT EXISTS myschema")
      conn.createStatement().execute(
        "CREATE TABLE IF NOT EXISTS myschema.prefixed_table (id INT, val VARCHAR(10))"
      )
      conn.createStatement().execute(
        "INSERT INTO myschema.prefixed_table SELECT 42, 'hello' " +
          "WHERE NOT EXISTS (SELECT 1 FROM myschema.prefixed_table WHERE id = 42)"
      )
      conn.close()

      val configWithSchema = validConfig.copy(
        id     = ID("h2_with_schema"),
        schema = Some(NonEmptyString.unsafeFrom("myschema"))
      )
      val jdbcConn = GenericJdbcConnection(configWithSchema)
      val df = jdbcConn.loadDataFrame(tableSource(
        "h2_with_schema",
        table = Some("prefixed_table")
      ))
      df.count() shouldEqual 1
    }

    "preserve various data types including NULL values" in {
      val conn = GenericJdbcConnection(validConfig)
      val df = conn.loadDataFrame(tableSource("h2_generic", table = Some("test_companies")))

      df.columns.map(_.toLowerCase) should contain allOf(
        "id", "name", "revenue", "is_active", "founded", "created_at", "rating", "description"
      )

      // Row with id=2 has NULL description
      val nullRow = df.filter("id = 2").select("description").collect().head
      nullRow.isNullAt(0) shouldEqual true

      // Non-null values are preserved
      val row1 = df.filter("id = 1").collect().head
      row1.getAs[Boolean]("IS_ACTIVE") shouldEqual true
    }

    "pass spark parameters through to JDBC options" in {
      val configWithParams = validConfig.copy(
        id         = ID("h2_with_params"),
        parameters = Seq(Refined.unsafeApply("fetchsize=10"))
      )
      val conn = GenericJdbcConnection(configWithParams)
      // If parameters caused an error, loadDataFrame would fail
      val df = conn.loadDataFrame(tableSource("h2_with_params", table = Some("test_companies")))
      df.count() shouldEqual 3
    }
  }
}
