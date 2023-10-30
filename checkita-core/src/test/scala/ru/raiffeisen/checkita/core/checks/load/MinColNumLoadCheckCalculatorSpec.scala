package ru.raiffeisen.checkita.core.checks.load

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.Common._
import ru.raiffeisen.checkita.core.CalculatorStatus
import ru.raiffeisen.checkita.core.checks.CommonChecksVals._
import ru.raiffeisen.checkita.readers.SchemaReaders.SourceSchema

class MinColNumLoadCheckCalculatorSpec extends AnyWordSpec with Matchers {
  
  private val schemas: Map[String, SourceSchema] = Map.empty
  
  "MinColNumLoadCheckCalculator" must {
    "return correct result for source with flat schema" in {
      MinColNumLoadCheckCalculator("min_col_check1", 3)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check2", 5)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check3", 6)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Failure
    }
    "return correct result for source with nested schema" in {
      MinColNumLoadCheckCalculator("min_col_check1", 1)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check2", 2)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check3", 3)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Failure
    }
  }
}
