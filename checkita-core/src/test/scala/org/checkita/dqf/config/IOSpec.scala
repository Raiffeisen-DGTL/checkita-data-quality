package org.checkita.dqf.config

import com.typesafe.config.ConfigRenderOptions
import eu.timepit.refined.api.Refined
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.config.IO.{readEncryptedJobConfig, readJobConfig, writeEncryptedJobConfig, writeJobConfig}
import org.checkita.dqf.config.Parsers.StringConfigParser
import org.checkita.dqf.config.jobconf.Connections.{GenericJdbcConnectionConfig, IcebergConnectionConfig}

import java.io.File

class IOSpec extends AnyWordSpec with Matchers {

  "Job Configuration" must {
    "be successfully read" in {
      val configFile = new File(getClass.getResource("/test_job.conf").getPath)
      val written = readJobConfig[File](configFile).flatMap(config => writeJobConfig(config)).map(_ => "Success")

      written shouldEqual Right("Success")
    }

    "parse GenericJdbcConnectionConfig fields correctly" in {
      val configFile = new File(getClass.getResource("/test_job.conf").getPath)
      val jobConfig = readJobConfig[File](configFile).toOption.get

      val jdbcConns = jobConfig.connections.map(_.jdbc).getOrElse(Seq.empty)
      jdbcConns should have size 2

      val trino = jdbcConns.find(_.id.value == "trino_conn").get
      trino.url.value          shouldEqual "jdbc:trino://trino-host:8080/hive/default"
      trino.driver.value       shouldEqual "io.trino.jdbc.TrinoDriver"
      trino.username.map(_.value) shouldEqual Some("dq-user")
      trino.password           shouldBe None
      trino.schema             shouldBe None

      val os = jdbcConns.find(_.id.value == "opensearch_conn").get
      os.url.value             shouldEqual "jdbc:opensearch://os-host:9200"
      os.driver.value          shouldEqual "org.opensearch.jdbc.Driver"
      os.username.map(_.value) shouldEqual Some("admin")
      os.password.map(_.value) shouldEqual Some("secret")
      os.schema.map(_.value)   shouldEqual Some("my_index")
      os.parameters            should have size 1
      os.parameters.head.value shouldEqual "fetchSize=1000"
    }

    "parse IcebergConnectionConfig fields correctly" in {
      val configFile = new File(getClass.getResource("/test_job.conf").getPath)
      val jobConfig = readJobConfig[File](configFile).toOption.get

      val icebergConns = jobConfig.connections.map(_.iceberg).getOrElse(Seq.empty)
      icebergConns should have size 2

      val hadoop = icebergConns.find(_.id.value == "iceberg_hadoop").get
      hadoop.catalogName.value    shouldEqual "test_catalog"
      hadoop.catalogType.value    shouldEqual "hadoop"
      hadoop.warehouse.map(_.value) shouldEqual Some("/tmp/iceberg/warehouse")
      hadoop.catalogUri           shouldBe None
      hadoop.parameters           shouldBe empty

      val hive = icebergConns.find(_.id.value == "iceberg_hive").get
      hive.catalogName.value    shouldEqual "hive_catalog"
      hive.catalogType.value    shouldEqual "hive"
      hive.warehouse.map(_.value) shouldEqual Some("s3a://bucket/warehouse")
      hive.catalogUri.map(_.value) shouldEqual Some("thrift://metastore:9083")
      hive.parameters            should have size 1
      hive.parameters.head.value shouldEqual "io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    }

    "parse IcebergSourceConfig fields correctly" in {
      val configFile = new File(getClass.getResource("/test_job.conf").getPath)
      val jobConfig = readJobConfig[File](configFile).toOption.get

      val icebergSources = jobConfig.sources.map(_.iceberg).getOrElse(Seq.empty)
      icebergSources should have size 2

      val src1 = icebergSources.find(_.id.value == "iceberg_source_1").get
      src1.connection.value   shouldEqual "iceberg_hadoop"
      src1.table.value        shouldEqual "events"
      src1.database.map(_.value) shouldEqual Some("analytics")
      src1.keyFields.map(_.value) shouldEqual Seq("event_id")

      val src2 = icebergSources.find(_.id.value == "iceberg_source_2").get
      src2.connection.value   shouldEqual "iceberg_hive"
      src2.table.value        shouldEqual "users"
      src2.database           shouldBe None
    }

    "be decrypted successfully" in {
      val encryptor = new ConfigEncryptor(
        Refined.unsafeApply("secretmustbeatleastthirtytwocharacters"), Seq("password")
      )

      val renderOpts = ConfigRenderOptions.defaults().setComments(false).setOriginComments(false).setFormatted(false)
      val configFile = new File(getClass.getResource("/test_job.conf").getPath)

      val written = readJobConfig[File](configFile).toOption.get

      val encryptedConfig = writeEncryptedJobConfig(written)(encryptor).toOption.get.root().render(renderOpts)
      val decryptedConfig = readEncryptedJobConfig[String](encryptedConfig)(StringConfigParser, encryptor).toOption.get

      val originalConfig = writeJobConfig(written).toOption.get.root().render(renderOpts)
      val afterDecryption = writeJobConfig(decryptedConfig).toOption.get.root().render(renderOpts)

      originalConfig shouldEqual afterDecryption
    }
  }
}