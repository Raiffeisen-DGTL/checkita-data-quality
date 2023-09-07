package ru.raiffeisen.checkita.targets

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.utils.enums.Targets
import ru.raiffeisen.checkita.utils.enums.Targets.TargetType

class TargetSpec extends AnyWordSpec with Matchers {
  "SystemTargetConfig" must {
    "have type of Targets.system" in {
      val systemTargetConfig = SystemTargetConfig("foo", Seq("bar", "baz"), Seq("qux", "lux"), new TargetConfig {
        override def getType: TargetType = Targets.hdfs
      })
      systemTargetConfig.getType shouldEqual Targets.system
    }
  }

  "HdfsTargetConfig" must {
    "have type of Targets.hdfs" in {
      val hdfsTargetConfig = HdfsTargetConfig("foo", "bar", "baz")
      hdfsTargetConfig.getType shouldEqual Targets.hdfs
    }
  }

  "HiveTargetConfig" must {
    "have type of Targets.hive" in {
      val hiveTargetConfig = HiveTargetConfig("foo", "bar")
      hiveTargetConfig.getType shouldEqual Targets.hive
    }
  }
}
