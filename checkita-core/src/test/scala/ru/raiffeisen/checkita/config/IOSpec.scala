package ru.raiffeisen.checkita.config

import com.typesafe.config.ConfigRenderOptions
import eu.timepit.refined.api.Refined
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import ru.raiffeisen.checkita.config.IO.{readEncryptedJobConfig, readJobConfig, writeEncryptedJobConfig, writeJobConfig}
import ru.raiffeisen.checkita.config.Parsers.StringConfigParser

import java.io.File

class IOSpec extends AnyWordSpec with Matchers {
  
  "Job Configuration" must {
    "be successfully read" in {
      val configFile = new File(getClass.getResource("/test_job.conf").getPath)
      val written = readJobConfig[File](configFile).flatMap(config => writeJobConfig(config)).map(_ => "Success")

      written shouldEqual Right("Success")
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