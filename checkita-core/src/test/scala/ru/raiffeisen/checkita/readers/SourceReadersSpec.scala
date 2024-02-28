package ru.raiffeisen.checkita.readers

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common._
import ru.raiffeisen.checkita.config.RefinedTypes.ID
import ru.raiffeisen.checkita.config.jobconf.Sources._
import ru.raiffeisen.checkita.connections.DQConnection
import ru.raiffeisen.checkita.core.Source
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema
import ru.raiffeisen.checkita.readers.SourceReaders._

class SourceReadersSpec extends AnyWordSpec with Matchers {
  private implicit val connections: Map[String, DQConnection] = Map.empty
  private implicit val schemas: Map[String, SourceSchema] = Map(
    "fixedFull" -> SourceSchema("fixedFull", StructType(Seq(
      StructField("rank", IntegerType, nullable = true),
      StructField("profile", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("revenue", StringType, nullable = true),
      StructField("growth_%", DoubleType, nullable = true),
      StructField("industry", StringType, nullable = true),
      StructField("workers", IntegerType, nullable = true),
      StructField("previous_workers", IntegerType, nullable = true),
      StructField("founded", IntegerType, nullable = true),
      StructField("yrs_on_list", IntegerType, nullable = true),
      StructField("metro", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
    )), Seq(5, 90, 60, 60, 6, 15, 11, 30, 10, 20, 10, 12, 40, 25)),
    "fixedShort" -> SourceSchema("fixedShort", StructType(Seq(
      StructField("rank", StringType, nullable = true),
      StructField("profile", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("revenue", StringType, nullable = true),
      StructField("growth_%", StringType, nullable = true),
      StructField("industry", StringType, nullable = true),
      StructField("workers", StringType, nullable = true),
      StructField("previous_workers", StringType, nullable = true),
      StructField("founded", StringType, nullable = true),
      StructField("yrs_on_list", StringType, nullable = true),
      StructField("metro", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
    )), Seq(5, 90, 60, 60, 6, 15, 11, 30, 10, 20, 10, 12, 40, 25)),
    "delimited" -> SourceSchema("delimited", StructType(Seq(
      StructField("rank", IntegerType, nullable = true),
      StructField("profile", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("revenue", StringType, nullable = true),
      StructField("growth_%", DoubleType, nullable = true),
      StructField("industry", StringType, nullable = true),
      StructField("workers", IntegerType, nullable = true),
      StructField("previous_workers", IntegerType, nullable = true),
      StructField("founded", IntegerType, nullable = true),
      StructField("yrs_on_list", IntegerType, nullable = true),
      StructField("metro", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
    ))),
    "avro" -> SourceSchema("delimited", StructType(Seq(
      StructField("rank", IntegerType, nullable = true),
      StructField("profile", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("url", StringType, nullable = true),
      StructField("state", StringType, nullable = true),
      StructField("revenue", StringType, nullable = true),
      StructField("growth_pct", DoubleType, nullable = true),
      StructField("industry", StringType, nullable = true),
      StructField("workers", IntegerType, nullable = true),
      StructField("previous_workers", IntegerType, nullable = true),
      StructField("founded", IntegerType, nullable = true),
      StructField("yrs_on_list", IntegerType, nullable = true),
      StructField("metro", StringType, nullable = true),
      StructField("city", StringType, nullable = true),
    )))
  )
  
  "FixedFileSourceReader" must {
    val filePath = getClass.getResource("/data/companies/inc_500_companies_2019.txt").getPath
    
    "correctly read fixed-width text file" in {
      val fixedFullSourceConfig = FixedFileSourceConfig(
        ID("fixedFullSource"), None, Refined.unsafeApply(filePath), Some(ID("fixedFull"))
      )
      val fixedShortSourceConfig = FixedFileSourceConfig(
        ID("fixedShortSource"), None, Refined.unsafeApply(filePath), Some(ID("fixedShort"))
      )
      
      val fixedFullSource = FixedFileSourceReader.read(fixedFullSourceConfig)
      val fixedShortSource = FixedFileSourceReader.read(fixedShortSourceConfig)

      fixedFullSource.isRight shouldEqual true
      fixedShortSource.isRight shouldEqual true

      val fixedFullDf = fixedFullSource.getOrElse(Source("error", spark.emptyDataFrame)).df
      val fixedShortDf = fixedShortSource.getOrElse(Source("error", spark.emptyDataFrame)).df
      
      fixedFullDf.schema shouldEqual schemas("fixedFull").schema
      fixedShortDf.schema shouldEqual schemas("fixedShort").schema

      fixedFullDf.count() shouldEqual 5012
      fixedShortDf.count() shouldEqual 5012
    }
    
    "return error when file not found" in {
      val sourceConfig = FixedFileSourceConfig(ID("fixedFullSource"), None, "some_file.txt", Some(ID("fixedFull")))
      FixedFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }

    "return error when schema not found" in {
      val sourceConfig = FixedFileSourceConfig(ID("fixedFullSource"), None, Refined.unsafeApply(filePath), Some(ID("some_schema")))
      FixedFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }
  }
  
  "DelimitedFileSourceReader" must {
    val fileWithHeader = getClass.getResource("/data/companies/inc_500_companies_2019.csv").getPath
    val fileWithoutHeader = getClass.getResource("/data/companies/inc_500_companies_2019_headless.csv").getPath

    "correctly read delimited text file" in {
      val sourceConfigHeader = DelimitedFileSourceConfig(
        ID("sourceHeader"), None, Refined.unsafeApply(fileWithHeader), header = true, schema = None
      )
      val sourceConfigHeadless = DelimitedFileSourceConfig(
        ID("sourceHeadless"), None, Refined.unsafeApply(fileWithoutHeader), schema = Some(ID("delimited"))
      )
      
      val sourceHeader = DelimitedFileSourceReader.read(sourceConfigHeader)
      val sourceHeadless = DelimitedFileSourceReader.read(sourceConfigHeadless)

      sourceHeader.isRight shouldEqual true
      sourceHeadless.isRight shouldEqual true

      val sourceHeaderDf = sourceHeader.getOrElse(Source("error", spark.emptyDataFrame)).df
      val sourceHeadlessDf = sourceHeadless.getOrElse(Source("error", spark.emptyDataFrame)).df

      sourceHeaderDf.schema shouldEqual schemas("fixedShort").schema
      sourceHeadlessDf.schema shouldEqual schemas("delimited").schema

      sourceHeaderDf.count() shouldEqual 5012
      sourceHeadlessDf.count() shouldEqual 5012
    }

    "return error when file not found" in {
      val sourceConfig = DelimitedFileSourceConfig(ID("delimitedSource"), None, "some_file.txt", header = true, schema = None)
      DelimitedFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }

    "return error when schema not found" in {
      val sourceConfig = DelimitedFileSourceConfig(
        ID("sourceHeadless"), None, Refined.unsafeApply(fileWithoutHeader), schema = Some(ID("some_schema"))
      )
      DelimitedFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }
    
    "return error when both header and schema are provided in source configuration or none of them is provided" in {
      val sourceConfig1 = DelimitedFileSourceConfig(
        ID("sourceConfig1"), None, Refined.unsafeApply(fileWithHeader), header = true, schema = Some(ID("some_schema"))
      )
      val sourceConfig2 = DelimitedFileSourceConfig(
        ID("sourceConfig2"), None, Refined.unsafeApply(fileWithHeader), schema = None
      )
      DelimitedFileSourceReader.read(sourceConfig1).isLeft shouldEqual true
      DelimitedFileSourceReader.read(sourceConfig2).isLeft shouldEqual true
    }
  }
  
  "AvroFileSourceReader" must {
    val avroFilePath = getClass.getResource("/data/companies/inc_500_companies_2019.avro").getPath

    "correctly read avro file" in {
      val sourceConfig = AvroFileSourceConfig(
        ID("avroSource"), None, Refined.unsafeApply(avroFilePath), schema = None
      )

      val source = AvroFileSourceReader.read(sourceConfig)

      source.isRight shouldEqual true

      val df = source.getOrElse(Source("error", spark.emptyDataFrame)).df
      
      df.schema shouldEqual schemas("avro").schema
      df.count() shouldEqual 5012
    }

    "correctly read avro file with schema" in {
      val sourceConfig = AvroFileSourceConfig(
        ID("avroSource"), None, Refined.unsafeApply(avroFilePath), schema = Some(ID("avro"))
      )

      val source = AvroFileSourceReader.read(sourceConfig)

      source.isRight shouldEqual true

      val df = source.getOrElse(Source("error", spark.emptyDataFrame)).df

      df.schema shouldEqual schemas("avro").schema
      df.count() shouldEqual 5012
    }
    
    "return error when file not found" in {
      val sourceConfig = AvroFileSourceConfig(ID("avroSource"), None, "some_file.txt", schema = None)
      AvroFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }

    "return error when schema provided but not found" in {
      val sourceConfig = AvroFileSourceConfig(
        ID("avroSource"), None, Refined.unsafeApply(avroFilePath), schema = Some(ID("some_schema"))
      )
      AvroFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }
  }
  
  "ParquetFileSourceReader" must {
    val parquetFilePath = getClass.getResource("/data/companies/inc_500_companies_2019.parquet").getPath

    "correctly read parquet file" in {
      val sourceConfig = ParquetFileSourceConfig(ID("parquetSource"), None, Refined.unsafeApply(parquetFilePath), None)

      val source = ParquetFileSourceReader.read(sourceConfig)

      source.isRight shouldEqual true

      val df = source.getOrElse(Source("error", spark.emptyDataFrame)).df

      df.schema shouldEqual schemas("delimited").schema
      df.count() shouldEqual 5012
    }

    "return error when file not found" in {
      val sourceConfig = ParquetFileSourceConfig(ID("parquetSource"), None, "some_file.txt", None)
      ParquetFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }
  }

  "OrcFileSourceReader" must {
    val orcFilePath = getClass.getResource("/data/companies/inc_500_companies_2019.orc").getPath

    "correctly read orc file" in {
      val sourceConfig = OrcFileSourceConfig(ID("orcSource"), None, Refined.unsafeApply(orcFilePath), None)

      val source = OrcFileSourceReader.read(sourceConfig)

      source.isRight shouldEqual true

      val df = source.getOrElse(Source("error", spark.emptyDataFrame)).df

      df.schema shouldEqual schemas("delimited").schema
      df.count() shouldEqual 5012
    }

    "return error when file not found" in {
      val sourceConfig = OrcFileSourceConfig(ID("orcSource"), None, "some_file.txt", None)
      OrcFileSourceReader.read(sourceConfig).isLeft shouldEqual true
    }
  }
}
