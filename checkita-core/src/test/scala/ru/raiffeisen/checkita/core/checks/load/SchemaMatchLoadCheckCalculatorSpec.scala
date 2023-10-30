package ru.raiffeisen.checkita.core.checks.load

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common._
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.checks.CommonChecksVals._
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema

class SchemaMatchLoadCheckCalculatorSpec extends AnyWordSpec with Matchers {
  
  
  
  "SchemaMatchLoadCheckCalculator" must {
    // all combinations: (schema_id, ignoreOrder, enableCaseSensitivity, status)
    // schema1 - exactly matches source one
    // schema2 - matches source one except columns' letters case
    // schema3 - matches source one but with different column order
    // schema4 - matches source one but with different column order and columns' letters case
    val allCombinations = Seq(
      ("schema1", false, false, CalculatorStatus.Success),
      ("schema1", false, true, CalculatorStatus.Success),
      ("schema1", true, false, CalculatorStatus.Success),
      ("schema1", true, true, CalculatorStatus.Success),
      ("schema2", false, false, CalculatorStatus.Success),
      ("schema2", false, true, CalculatorStatus.Failure),
      ("schema2", true, false, CalculatorStatus.Success),
      ("schema2", true, true, CalculatorStatus.Failure),
      ("schema3", false, false, CalculatorStatus.Failure),
      ("schema3", false, true, CalculatorStatus.Failure),
      ("schema3", true, false, CalculatorStatus.Success),
      ("schema3", true, true, CalculatorStatus.Success),
      ("schema4", false, false, CalculatorStatus.Failure),
      ("schema4", false, true, CalculatorStatus.Failure),
      ("schema4", true, false, CalculatorStatus.Success),
      ("schema4", true, true, CalculatorStatus.Failure),
    )
    
    "return correct result for source with flat schema" in {
      val schema1 = SourceSchema("schema1", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("someValue1", StringType, nullable = true),
        StructField("otherValue2", StringType, nullable = true),
        StructField("dateTime", StringType, nullable = true)
      )))
      val schema2 = SourceSchema("schema2", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("somevalue1", StringType, nullable = true),
        StructField("othervalue2", StringType, nullable = true),
        StructField("datetime", StringType, nullable = true)
      )))
      val schema3 = SourceSchema("schema3", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("dateTime", StringType, nullable = true),
        StructField("someValue1", StringType, nullable = true),
        StructField("otherValue2", StringType, nullable = true)
      )))
      val schema4 = SourceSchema("schema4", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true),
        StructField("datetime", StringType, nullable = true),
        StructField("somevalue1", StringType, nullable = true),
        StructField("othervalue2", StringType, nullable = true)
      )))
      val schemas = Seq(schema1, schema2, schema3, schema4).map(s => s.id -> s).toMap
      
      allCombinations.foreach(t =>
        SchemaMatchLoadCheckCalculator("check", t._1, t._2)
          .run(flatSrc, schemas)(settings.copy(enableCaseSensitivity = t._3)).status shouldEqual t._4
      )
    }
    "return correct result for source with nested schema" in {
      val schema1 = SourceSchema("schema1", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("data", StructType(Seq(
          StructField("name", StringType, nullable = true),
          StructField("dateTime", StringType, nullable = true),
          StructField("values", StructType(Seq(
            StructField("someValue1", StringType, nullable = true),
            StructField("otherValue2", StringType, nullable = true)
          )))
        )))
      )))
      val schema2 = SourceSchema("schema2", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("data", StructType(Seq(
          StructField("name", StringType, nullable = true),
          StructField("datetime", StringType, nullable = true),
          StructField("values", StructType(Seq(
            StructField("somevalue1", StringType, nullable = true),
            StructField("othervalue2", StringType, nullable = true)
          )))
        )))
      )))
      val schema3 = SourceSchema("schema3", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("data", StructType(Seq(
          StructField("dateTime", StringType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("values", StructType(Seq(
            StructField("otherValue2", StringType, nullable = true),
            StructField("someValue1", StringType, nullable = true)
          )))
        )))
      )))
      val schema4 = SourceSchema("schema4", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("data", StructType(Seq(
          StructField("datetime", StringType, nullable = true),
          StructField("name", StringType, nullable = true),
          StructField("values", StructType(Seq(
            StructField("othervalue2", StringType, nullable = true),
            StructField("somevalue1", StringType, nullable = true)
          )))
        )))
      )))
      val schemas = Seq(schema1, schema2, schema3, schema4).map(s => s.id -> s).toMap

      allCombinations.foreach(t =>
        SchemaMatchLoadCheckCalculator("check", t._1, t._2)
          .run(nestedSrc, schemas)(settings.copy(enableCaseSensitivity = t._3)).status shouldEqual t._4
      )
    }
  }
}
