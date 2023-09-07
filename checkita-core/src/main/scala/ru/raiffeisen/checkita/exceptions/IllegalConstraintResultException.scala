package ru.raiffeisen.checkita.exceptions

case class IllegalConstraintResultException(checkId: String) extends Exception(
  s"Check result is in an illegal state: CheckId = $checkId"
)

case class IllegalParameterException(params: String) extends Exception(
  s"Unknown parameters = $params"
)

case class MissingParameterInException(where: String) extends Exception(
  s"Parameters missing in $where"
)
