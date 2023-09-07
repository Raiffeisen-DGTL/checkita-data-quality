package ru.raiffeisen.checkita.utils

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.raiffeisen.checkita.utils.enums.ConfigEnum


object ConfigEnumSample extends ConfigEnum {
  val foo: Value = Value("foo")
  val bar: Value = Value("bar")
  val baz: Value = Value("baz")
}

class enumsSpec extends AnyWordSpec with Matchers {

  "The ConfigEnum object" must {
    "have <names> getter which returns values as a set of strings" in {
      ConfigEnumSample.names shouldBe a [Set[_]]
      ConfigEnumSample.names should have size 3
      ConfigEnumSample.names should contain ("foo")
      ConfigEnumSample.names should contain ("bar")
      ConfigEnumSample.names should contain ("baz")
    }

    "have <withNameOpt> methods which finds a value by its text representation and returns and Option[Value]" in {
      ConfigEnumSample.withNameOpt("bar") shouldEqual Some(ConfigEnumSample.bar)
      ConfigEnumSample.withNameOpt("qux") shouldEqual None
    }

    "have <contains> methods which checks if value exists in enumeration by its text representation" in {
      ConfigEnumSample.contains("bar") shouldEqual true
      ConfigEnumSample.contains("qux") shouldEqual false
    }
  }
}
