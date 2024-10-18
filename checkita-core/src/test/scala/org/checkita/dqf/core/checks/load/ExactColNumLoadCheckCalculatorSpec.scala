package org.checkita.dqf.core.checks.load

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.Common._
import org.checkita.dqf.core.CalculatorStatus
import org.checkita.dqf.core.checks.CommonChecksVals._
import org.checkita.dqf.readers.SchemaReaders.SourceSchema

class ExactColNumLoadCheckCalculatorSpec extends AnyWordSpec with Matchers {
  
  private val schemas: Map[String, SourceSchema] = Map.empty
  
  "ExactColNumLoadCheckCalculator" must {
    "return correct result for source with flat schema" in {
      val test = ExactColNumLoadCheckCalculator("exact_col_check", 5)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Success
      ExactColNumLoadCheckCalculator("exact_col_check", 2)
        .run(flatSrc, schemas).status shouldEqual CalculatorStatus.Failure
    }
    "return correct result for source with nested schema" in {
      ExactColNumLoadCheckCalculator("exact_col_check", 2)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Success
      ExactColNumLoadCheckCalculator("exact_col_check", 5)
        .run(nestedSrc, schemas).status shouldEqual CalculatorStatus.Failure
    }
  }
}
