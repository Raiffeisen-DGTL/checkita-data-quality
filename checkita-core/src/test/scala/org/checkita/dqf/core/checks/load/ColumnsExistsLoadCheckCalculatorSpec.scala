package org.checkita.dqf.core.checks.load

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.checks.CommonChecksVals._
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

class ColumnsExistsLoadCheckCalculatorSpec extends AnyWordSpec with Matchers {
  
  private val schemas: Map[String, SourceSchema] = Map.empty
  
  "ColumnsExistsLoadCheckCalculator" must {
    val caseInsensitiveCheck = ColumnsExistsLoadCheckCalculator(
      "col_exists_check", Seq("name", "somevalue1", "datetime")
    )
    val caseSensitiveCheck = ColumnsExistsLoadCheckCalculator(
      "col_exists_check", Seq("name", "someValue1", "dateTime")
    )
    
    "return correct result for source with flat schema" in {
      caseInsensitiveCheck.run(flatSrc, schemas).status shouldEqual CalculatorStatus.Success
      
      caseInsensitiveCheck.run(flatSrc, schemas)(
        settings.copy(enableCaseSensitivity = true)
      ).status shouldEqual CalculatorStatus.Failure
      
      caseSensitiveCheck.run(flatSrc, schemas)(
        settings.copy(enableCaseSensitivity = true)
      ).status shouldEqual CalculatorStatus.Success
    }
    "return correct result for source with nested schema" in {
      caseInsensitiveCheck.run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Success

      caseInsensitiveCheck.run(nestedSrc, schemas)(
        settings.copy(enableCaseSensitivity = true)
      ).status shouldEqual CalculatorStatus.Failure

      caseSensitiveCheck.run(nestedSrc, schemas)(
        settings.copy(enableCaseSensitivity = true)
      ).status shouldEqual CalculatorStatus.Success
    }
  }

}
