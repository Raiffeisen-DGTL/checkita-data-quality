package ru.raiffeisen.checkita.utils

import scala.util.{Failure, Success, Try}

object ResultUtils {

  /**
   * Type alias for result of any type wrapped in Either.
   * Idea here is to collect any errors in form of log messages to process them later
   * @tparam T Result type
   */
  type Result[T] = Either[Vector[String], T]

  /**
   * Wraps value into Result
   * @param value value to wrap
   * @tparam T Type of value
   * @return Result of value type
   */
  def liftToResult[T](value: T): Result[T] = Right(value).toResult(Vector.empty)
  
  /**
   * Implicit conversion for Try[T] with extra methods
   * @param value Try value
   * @tparam T Type of Try value
   */
  implicit class TryOps[T](value: Try[T]) {

    /**
     * Converts Try[T] into a Result[T]
     * @param preMsg Additional descriptive message added to error message
     * @param includeStackTrace Flag indicating whether to include error stack trace into log message
     * @return Result[T] with either result of error log message
     */
    def toResult(preMsg: String = "", includeStackTrace: Boolean = false): Result[T] =
      value match {
        case Success(v) => Right(v)
        case Failure(e) =>
          val msg = preMsg + "\n" + e.getMessage +
            (if (includeStackTrace) e.getStackTrace.mkString("\n", "\n", "") else "")
          Left(Vector(msg))
      }
  }

  /**
   * Implicit conversion for Either[T] to enable conversion to Result[T]
   * @param value Either value
   * @tparam L Left type
   * @tparam R Right Type
   */
  implicit class EitherOps[L, R](value: Either[L, R]) {
    /**
     * Transforms left value of either. If values is Right(_), then returns it unchanged.
     * @param f Function to transform left value to a new value
     * @tparam LL Type of new left value
     * @return Either with transformed left value.
     */
    def mapLeft[LL](f: L => LL): Either[LL, R] = value match {
      case Right(r) => Right(r)
      case Left(l) => Left(f(l))
    }

    /**
     * Converts either value to a Result value.
     * @param f Function that converts left value to Vector of log messages: Vector[LogMsg]
     * @return Result value
     */
    def toResult(f: L => Vector[String]): Result[R] = mapLeft[Vector[String]](f)
  }
  
  /**
   * Implicit conversion for Result[T] with some extra methods
   * @param value result value
   * @tparam T Type of result value
   */
  implicit class ResultOps[T](value: Result[T]) {

    /**
     * Maps value in case of a valid result.
     * @param f Function to transform a value
     * @tparam R Type of the new result
     * @return New result: either a mapped value or an unchanged log messages.
     */
    def mapValue[R](f: T => R): Result[R] = value match {
      case Right(v) => Try(f(v)).toResult()
      case Left(logs) => Left(logs)
    }

    /**
     * Applies functions with side effects to this result.
     * @param f Function with side effects to apply to this result if it contains value
     * @param g Function with side effects to apply to this result if it contains errors messages.
     * @return Returns the same result if function execution was successful.
     *         Otherwise returns log messages with errors.
     */
    def tap(f: T => Unit, g: Vector[String] => Unit = _ => ()): Result[T] = value match {
      case Right(v) => Try(f(v)).toResult().mapValue(_ => v)
      case Left(logs) => Try(g(logs)); Left(logs)
    }
    
    /**
     * Combines results and their log messages
     * @param otherValue other result to combine with
     * @param f Function to combine two results
     * @tparam R Type of other result
     * @tparam S Type of combined result
     * @return Combined result: either a combined value or a combined error log messages
     */
    def combine[R, S](otherValue: Result[R])(f: (T, R) => S): Result[S] =
      (value, otherValue) match {
        case (Right(v1), Right(v2)) => Right(f(v1, v2))
        case (Left(l1), Left(l2)) => Left(l1 ++ l2)
        case (Left(l1), _) => Left(l1)
        case (_, Left(l2)) => Left(l2)
      }

    /**
     * Combines this result with two other results provided with function to combine values.
     * @param r1 Result 1 to combine with
     * @param r2 Result 2 to combine with
     * @param f Function to combine results values
     * @tparam R1 Type of Result 1
     * @tparam R2 Type of Result 2
     * @tparam S Type of combined result
     * @return Combined result: either a combined value or a combined error log messages
     */
    def combineT2[R1, R2, S](r1: Result[R1], r2: Result[R2])(f: (T, R1, R2) => S): Result[S] = {
      val g1 = (v1: R1, v2: R2) => (v1, v2)
      val g2: (T, (R1, R2)) => S = (v, t) => f(v, t._1, t._2)
      value.combine(r1.combine(r2)(g1))(g2)
    }

    /**
     * Combines this result with three other results provided with function to combine values.
     * @param r1 Result 1 to combine with
     * @param r2 Result 2 to combine with
     * @param r3 Result 3 to combine with
     * @param f Function to combine results values
     * @tparam R1 Type of Result 1
     * @tparam R2 Type of Result 2
     * @tparam R3 Type of Result 3
     * @tparam S Type of combined result
     * @return Combined result: either a combined value or a combined error log messages
     */
    def combineT3[R1, R2, R3, S](r1: Result[R1], r2: Result[R2], r3: Result[R3])(f: (T, R1, R2, R3) => S): Result[S] = {
      val g1 = (v1: R1, v2: R2, v3: R3) => (v1, v2, v3)
      val g2: (T, (R1, R2, R3)) => S = (v, t) => f(v, t._1, t._2, t._3)
      value.combine(r1.combineT2(r2, r3)(g1))(g2)
    }

    /**
     * Combines this result with three other results provided with function to combine values.
     * @param r1 Result 1 to combine with
     * @param r2 Result 2 to combine with
     * @param r3 Result 3 to combine with
     * @param r4 Result 4 to combine with
     * @param f Function to combine results values
     * @tparam R1 Type of Result 1
     * @tparam R2 Type of Result 2
     * @tparam R3 Type of Result 3
     * @tparam R4 Type of Result 4
     * @tparam S Type of combined result
     * @return Combined result: either a combined value or a combined error log messages
     */
    def combineT4[R1, R2, R3, R4, S](r1: Result[R1], r2: Result[R2], r3: Result[R3], r4: Result[R4])
                                    (f: (T, R1, R2, R3, R4) => S): Result[S] = {
      val g1 = (v1: R1, v2: R2, v3: R3, v4: R4) => (v1, v2, v3, v4)
      val g2: (T, (R1, R2, R3, R4)) => S = (v, t) => f(v, t._1, t._2, t._3, t._4)
      value.combine(r1.combineT3(r2, r3, r4)(g1))(g2)
    }

    def combineT5[R1, R2, R3, R4, R5, S](r1: Result[R1], r2: Result[R2], r3: Result[R3], r4: Result[R4], r5: Result[R5])
                                        (f: (T, R1, R2, R3, R4, R5) => S): Result[S] = {
      val g1 = (v1: R1, v2: R2, v3: R3, v4: R4, v5: R5) => (v1, v2, v3, v4, v5)
      val g2: (T, (R1, R2, R3, R4, R5)) => S = (v, t) => f(v, t._1, t._2, t._3, t._4, t._5)
      value.combine(r1.combineT4(r2, r3, r4, r5)(g1))(g2)
    }
  }
}
