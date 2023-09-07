package ru.raiffeisen.checkita.checks

object CheckStatusEnum extends Enumeration {
  type CheckResultStatus = Value
  val Success, Failure, Error = Value
}

sealed trait CheckStatus {
  val stringValue: String
}

object CheckSuccess extends CheckStatus {
  val stringValue = "Success"
}

object CheckFailure extends CheckStatus {
  val stringValue = "Failure"
}

case class CheckError(throwable: Throwable) extends CheckStatus {
  val stringValue = "Error"
}
