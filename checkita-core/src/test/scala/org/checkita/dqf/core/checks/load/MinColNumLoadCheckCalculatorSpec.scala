package org.checkita.dqf.core.checks.load

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.checks.CommonChecksVals._
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

class MinColNumLoadCheckCalculatorSpec extends AnyWordSpec with Matchers {
  
  private val schemas: Map[String, SourceSchema] = Map.empty
  
  "MinColNumLoadCheckCalculator" must {
    "return correct result for source with flat schema" in {
      MinColNumLoadCheckCalculator("min_col_check1", 3, isCritical = false)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check2", 5, isCritical = false)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check3", 6, isCritical = false)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Failure
    }
    "return correct result for source with nested schema" in {
      MinColNumLoadCheckCalculator("min_col_check1", 1, isCritical = false)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check2", 2, isCritical = false)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Success
      MinColNumLoadCheckCalculator("min_col_check3", 3, isCritical = false)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Failure
    }
  }
}
