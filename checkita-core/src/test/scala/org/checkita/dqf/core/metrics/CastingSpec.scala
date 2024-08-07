package org.checkita.dqf.core.metrics

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.checkita.dqf.core.metrics.rdd.Casting._

import java.nio.ByteBuffer
import java.time.{LocalDate, LocalDateTime, LocalTime}

class CastingSpec extends AnyWordSpec with Matchers {


  private val seq1 = Seq(0, 3, 8, 4, 0, 5, 5, 8, 9, 3, 2, 2, 6, 2, 6)
  private val seq2 = Seq(7.28, 6.83, 3.0, 2.0, 6.66, 9.03, 3.69, 2.76, 4.64, 7.83, 9.19, 4.0, 7.5, 3.87, 1.0)
  private val seq3 = Seq("7.24", "9.74", "8.32", "9.15", "5.0", "8.38", "2.0", "3.42", "3.0", "6.04", "1.0", "8.37", "0.9", "1.0", "6.54")
  private val seq4 = Seq(4, 3.14, "foo", 3.0, -25.321, "bar", "[12, 35]", true, 'd', '3', "34.12", "2.0", "3123dasd", 42, "4")

  private val emptyArray: Array[Byte] = Array.empty
  private val byteNumArr: Array[Byte] = Array(42.toByte)
  private val shortNumArr: Array[Byte] = ByteBuffer.allocate(2).putShort(7777.toShort).array()
  private val intNumArr: Array[Byte] = ByteBuffer.allocate(4).putInt(-123321).array()
  private val doubleNumArr: Array[Byte] = ByteBuffer.allocate(8).putDouble(3.1415926535).array()
  private val longNumArr: Array[Byte] = ByteBuffer.allocate(8).putLong(321147000987L).array()
  private val strNumArr: Array[Byte] = "2.71828".getBytes
  private val strNonNum: Array[Byte] = "foo".getBytes
  private val fourSymbolStr: Array[Byte] = "3.14".getBytes

  "getDoubleFromBytes" must {
    "return None for empty byte array" in {
      getDoubleFromBytes(emptyArray) shouldEqual None
    }
    "return valid double for array of single byte" in {
      getDoubleFromBytes(byteNumArr) shouldEqual Some(42.0)
    }
    "return valid double for array of two bytes" in {
      getDoubleFromBytes(shortNumArr) shouldEqual Some(7777.0)
    }
    "return valid double for array of four bytes" in {
      getDoubleFromBytes(intNumArr) shouldEqual Some(-123321.0)
    }
    "return valid double for array of eight bytes" in {
      getDoubleFromBytes(doubleNumArr) shouldEqual Some(3.1415926535)
    }
    "return valid double for array of arbitrary length encoding numeric string" in {
      getDoubleFromBytes(strNumArr) shouldEqual Some(2.71828)
    }
    "return valid double for array of arbitrary length encoding numeric string in sientific format" in {
      getDoubleFromBytes("1.23e3".getBytes) shouldEqual Some(1230.0)
    }
    "return None for array of arbitrary length encoding non-numeric string" in {
      getDoubleFromBytes(strNonNum) shouldEqual None
    }
    "return some double in case if array encodes some string and have a length that matches number representation" in {
      val res = getDoubleFromBytes(fourSymbolStr)

      res.isDefined shouldEqual true
      res.get should not equal 3.14
    }
  }

  "getLongFromBytes" must {
    "return None for empty byte array" in {
      getDoubleFromBytes(emptyArray) shouldEqual None
    }
    "return valid long for array of single byte" in {
      getLongFromBytes(byteNumArr) shouldEqual Some(42L)
    }
    "return valid double for array of two bytes" in {
      getLongFromBytes(shortNumArr) shouldEqual Some(7777L)
    }
    "return valid double for array of four bytes" in {
      getLongFromBytes(intNumArr) shouldEqual Some(-123321L)
    }
    "return valid double for array of eight bytes" in {
      getLongFromBytes(longNumArr) shouldEqual Some(321147000987L)
    }
    "return valid double for array of arbitrary length encoding numeric string" in {
      getLongFromBytes(strNumArr) shouldEqual Some(2)
    }
    "return None for array of arbitrary length encoding non-numeric string" in {
      getLongFromBytes(strNonNum) shouldEqual None
    }
    "return some double in case if array encodes some string and have a length that matches number representation" in {
      val res = getLongFromBytes(fourSymbolStr)

      res.isDefined shouldEqual true
      res.get should not equal 3
    }
  }

  "stringToLocalDateTime" must {
    "return correct local date time when provided with both valid string and valid format string" in {
      stringToLocalDateTime("2024-02-22 13:32:18", "yyyy-MM-dd HH:mm:ss") shouldEqual Some(LocalDateTime.of(
        2024, 2, 22, 13, 32, 18, 0
      ))
    }
    "return correct local date time when provided with date string and format" in {
      stringToLocalDateTime("22-02-2024", "dd-MM-yyyy") shouldEqual Some(LocalDateTime.of(
        2024, 2, 22, 0, 0, 0, 0
      ))
    }
    "return correct local date time when provided with time string and format (time at current day)" in {
      stringToLocalDateTime("13:32:18", "HH:mm:ss") shouldEqual Some(
        LocalTime.of(13, 32, 18).atDate(LocalDate.now())
      )
    }
    "return None when datetime string do not match format" in {
      stringToLocalDateTime("13:32:18 22-02-2024", "yyyy-MM-dd HH:mm:ss") shouldEqual None
    }
    "return None when string matches format but contains illegal values" in {
      stringToLocalDateTime("2024-02-22 99:99:99", "yyyy-MM-dd HH:mm:ss") shouldEqual None
    }
  }


}
