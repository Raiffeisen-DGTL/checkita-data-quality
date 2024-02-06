package ru.raiffeisen.checkita.readers

import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import org.apache.avro.Schema.Parser
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.config.jobconf.Schemas._
import ru.raiffeisen.checkita.Common._
import ru.raiffeisen.checkita.config.RefinedTypes.ID
import ru.raiffeisen.checkita.readers.SchemaReaders._

import java.io.File
import scala.util.Try

class SchemaReadersSpec extends AnyWordSpec with Matchers {

  "DelimitedSchemaReader" must {
    "correctly read schema when provided with delimited schema config" in {
      val schema = DelimitedSchemaConfig(ID("schema"), Refined.unsafeApply(Seq(
        GeneralColumn("col1", StringType),
        GeneralColumn("col2", IntegerType),
        GeneralColumn("col3", DoubleType)
      )))
      
      val schemaResult = SourceSchema("schema", StructType(Seq(
        StructField("col1", StringType, nullable = true),
        StructField("col2", IntegerType, nullable = true),
        StructField("col3", DoubleType, nullable = true)
      )))
      
      val schemaRead = DelimitedSchemaReader.read(schema)
      
      schemaRead.isRight shouldEqual true
      schemaRead.getOrElse(SourceSchema("error", StructType(Seq.empty))) shouldEqual schemaResult
    }
  }
  
  "FixedFullSchemaReader" must {
    "correctly read schema when provided with fixed full schema config" in {
      val schema = FixedFullSchemaConfig(ID("schema"), Refined.unsafeApply(Seq(
        FixedFullColumn("col1", StringType, 5),
        FixedFullColumn("col2", IntegerType, 3),
        FixedFullColumn("col3", DoubleType, 8),
      )))
      
      val schemaResult = SourceSchema("schema", StructType(Seq(
        StructField("col1", StringType, nullable = true),
        StructField("col2", IntegerType, nullable = true),
        StructField("col3", DoubleType, nullable = true)
      )), Seq(5, 3, 8))
      
      val schemaRead = FixedFullSchemaReader.read(schema)

      schemaRead.isRight shouldEqual true
      schemaRead.getOrElse(SourceSchema("error", StructType(Seq.empty))) shouldEqual schemaResult
    }
  }
  
  "FixedShortSchemaReader" must {
    "correctly read schema when provided with fixed short schema config" in {
      val schema = FixedShortSchemaConfig(ID("schema"), Refined.unsafeApply(Seq(
        "c-o-l-1:34", "c_o_l_2:27", "c o l 3:31", "c$o`l@4:21"
      )))

      val schemaResult = SourceSchema("schema", StructType(Seq(
        StructField("c-o-l-1", StringType, nullable = true),
        StructField("c_o_l_2", StringType, nullable = true),
        StructField("c o l 3", StringType, nullable = true),
        StructField("c$o`l@4", StringType, nullable = true),
      )), Seq(34, 27, 31, 21))

      val schemaRead = FixedShortSchemaReader.read(schema)

      schemaRead.isRight shouldEqual true
      schemaRead.getOrElse(SourceSchema("error", StructType(Seq.empty))) shouldEqual schemaResult
    }
  }
  
  "AvroSchemaReader" must {
    "correctly read schema when provided with avro schema config" in {
      val fileToString: String => Option[String] = path => Try {
        val schemaParser = new Parser()
        val avroSchema = schemaParser.parse(new File(path))
        avroSchema.toString
      }.toOption

      val schema1Path = getClass.getResource("/test_schema1.avsc").getPath
      val schema2Path = getClass.getResource("/test_schema2.avsc").getPath
      val schema1 = AvroSchemaConfig(ID("schema1"), Refined.unsafeApply(schema1Path))
      val schema2 = AvroSchemaConfig(ID("schema2"), Refined.unsafeApply(schema2Path))

      val schema1Result = SourceSchema("schema1", StructType(Seq(
        StructField("column1", StringType, nullable = true),
        StructField("column2", IntegerType, nullable = true),
        StructField("column3", DoubleType, nullable = true)
      )), Seq.empty, fileToString(schema1Path))

      val schema2Result = SourceSchema("schema2", StructType(Seq(
        StructField("column1", StringType, nullable = true),
        StructField("column2", StructType(Seq(
          StructField("sub_column1", StringType, nullable = false),
          StructField("sub_column2", DoubleType, nullable = false)
        )), nullable = false),
        StructField("column3", DoubleType, nullable = true)
      )), Seq.empty, fileToString(schema2Path))

      val schema1Read = AvroSchemaReader.read(schema1)
      val schema2Read = AvroSchemaReader.read(schema2)
      
      schema1Read.isRight shouldEqual true
      schema2Read.isRight shouldEqual true
      
      schema1Read.getOrElse(SourceSchema("error", StructType(Seq.empty))) shouldEqual schema1Result
      schema2Read.getOrElse(SourceSchema("error", StructType(Seq.empty))) shouldEqual schema2Result
    }
    
    "return error when file with avro schema is not found" in {
      val schema = AvroSchemaConfig(ID("schema1"), "some_avro_schema.avsc")
      AvroSchemaReader.read(schema).isLeft shouldEqual true
    }
  }
}
