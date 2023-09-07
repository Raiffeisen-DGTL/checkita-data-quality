package ru.raiffeisen.checkita.checks.sql

import ru.raiffeisen.checkita.exceptions.IllegalParameterException

import java.sql.ResultSet

object SQLCheckProcessor {
  
  /**
   * Selecting proper transformation function
   * @param checkType type of check ("COUNT_EQ_ZERO", "COUNT_NOT_EQ_ZERO",...)
   * @return transformation fuctions (for results and for check)
   */
  def getTransformations(checkType: String): (ResultSet => Int, Int => Boolean) = {
    val controlType = StandardControl.withName(checkType)
    controlType match {
      case StandardControl.COUNT_EQ_ZERO | StandardControl.COUNT_NOT_EQ_ZERO =>
        (ConfigUtils.countTransform, StandardControl.getControl(controlType))
    }
  }

}

/**
 * Enumeration to store mapping fuctions
 */
object StandardControl extends Enumeration {
  val COUNT_EQ_ZERO: StandardControl.Value = Value("COUNT_EQ_ZERO")
  val COUNT_NOT_EQ_ZERO: StandardControl.Value = Value("COUNT_NOT_EQ_ZERO")

  def getControl(c: StandardControl.Value): Int => Boolean = c match {
    case StandardControl.COUNT_EQ_ZERO =>
      (a: Int) =>
        a == 0
    case StandardControl.COUNT_NOT_EQ_ZERO =>
      (a: Int) =>
        a != 0
    case x => throw IllegalParameterException(x.toString)
  }
}

/**
 * Collection of ResultSet processing functions. Assuming, that in some case
 * other functions will be needed
 */
object ConfigUtils {
  def countTransform: ResultSet => Int = (rs: ResultSet) => {
    rs.next
    rs.getInt(1)
  }
}
