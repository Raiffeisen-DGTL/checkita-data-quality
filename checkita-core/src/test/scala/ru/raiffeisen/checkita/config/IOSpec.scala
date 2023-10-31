package ru.raiffeisen.checkita.config

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.config.IO.{readJobConfig, writeJobConfig}

import java.io.File

class IOSpec extends AnyWordSpec with Matchers {
  
  "Job Configuration" must {
    "be successfully read" in {
      val configFile = new File(getClass.getResource("/test_job.conf").getPath)
      val written = readJobConfig[File](configFile).flatMap(config => writeJobConfig(config)).map(_ => "Success")

      written shouldEqual Right("Success")
      
    }
  }
}