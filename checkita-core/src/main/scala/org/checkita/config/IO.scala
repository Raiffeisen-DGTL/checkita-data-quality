package org.checkita.config

import com.typesafe.config.{Config, ConfigRenderOptions}
import pureconfig.generic.auto.{exportReader, exportWriter}
import pureconfig.{ConfigReader, ConfigSource, ConfigWriter}
import org.checkita.config.Parsers._
import org.checkita.config.appconf.AppConfig
import org.checkita.config.jobconf.JobConfig
import org.checkita.config.validation.PostValidation.allPostValidations
import org.checkita.utils.ResultUtils._

import scala.util.Try

// required implicits:
import eu.timepit.refined.pureconfig._
import org.checkita.config.Implicits._
import org.checkita.config.validation.PreValidation._

object IO {

  /**
   * Safely writes Data Quality configuration into TypeSafe Config.
   * @param config Data Quality configuration
   * @param root Root key at which configuration will be written.
   * @param confName Data Quality configuration name (either application or job)
   * @param cw Implicit PureConfig writer for configuration of type T
   * @tparam T Type of Data Quality configuration (either AppConfig or JobConfig)
   * @return Either TypeSafe Config object with Data Quality configuration
   *         or a list with error occurred during write.
   */  
  private def write[T](config: T, root: String, confName: String)
                      (implicit cw: ConfigWriter[T]): Result[Config] = 
    Try(cw.to(config).atKey(root)).toResult(
      preMsg = s"Unable to write Data Quality $confName configuration to TypeSafe Config object with following error:",
      includeStackTrace = false
    )

  /**
   * Safely writes Data Quality application configuration into TypeSafe Config.
   * @param config Instance of Data Quality application configuration
   * @return Either TypeSafe Config object with Data Quality application configuration
   *         or a list with error occurred during write.
   */
  def writeAppConfig(config: AppConfig): Result[Config] =
    write[AppConfig](config, "appConfig", "application")

  /**
   * Safely writes Data Quality job configuration into TypeSafe Config.
   * @param config Instance of Data Quality job configuration
   * @return Either TypeSafe Config object with Data Quality job configuration
   *         or a list with error occurred during write.
   */
  def writeJobConfig(config: JobConfig): Result[Config] =
    write[JobConfig](config, "jobConfig", "job")

  /**
   * Safely writes Data Quality Job configuration into TypeSafe Config and then
   * encrypts all sensitive fields (including spark parameters).
   *
   * @param config    Instance of Data Quality job configuration
   * @param encryptor Implicit configuration encryptor
   * @return Either TypeSafe Config object with encrypted Data Quality job configuration
   *         or a list with error occurred during write or encryption process.
   */
  def writeEncryptedJobConfig(config: JobConfig)(implicit encryptor: ConfigEncryptor): Result[Config] =
    writeJobConfig(config).flatMap(encryptor.encryptConfig)

  /**
   * Reads Data Quality configuration into TypeSafe config object.
   * @param input Data Quality configuration input
   * @param confName Data Quality configuration name (either application or job)
   * @param p Implicit TypeSafe config parser for input of type T
   * @tparam T Type of the input
   * @return Either configuration in form of TypeSafe config object or a list of read errors.
   */
  private def read[T](input: T, confName: String)
                     (implicit p: ConfigParser[T]): Result[Config] =
    Try(p.parse(input)).toResult(
      preMsg = s"Unable to read Data Quality $confName configuration into TypeSafe config object due to following error:",
      includeStackTrace = false
    )

  /**
   * Parses TypeSafe config object into an instance of Data Quality configuration.
   * @param config TypeSafe config object with Data Quality configuration
   * @param confName Data Quality configuration name (either application or job)
   * @param root Root key at which to start parsing configuration
   * @param cr Implicit PureConfig reader for configuration of type T
   * @tparam T Type of configuration (either AppConfig or JobConfig)
   * @return Either parsed instance of configuration or a list of parsing errors.
   */
  private def parse[T](config: Config, confName: String, root: String)
                      (implicit cr: ConfigReader[T]): Result[T] = 
    ConfigSource.fromConfig(config).at(root).load[T].toResult(f => 
      s"Unable to parse Data Quality $confName configuration due to following errors:" +: f.toList.map(_.prettify).toVector
    )

  /**
   * Runs post validation for parsed Data Quality job configuration.
   * It worth mentioning that post validation are run for TypeSage Config object
   * as it allows more straightforward iteration over its elements.
   * For this purpose parsed configuration is written back to TypeSafe Config
   * prior running post validation checks.
   * @param jobConfig Parsed Data Quality job configuration
   * @return Either Data Quality job configuration or a list of errors caused by post validation checks
   */
  private def runPostValidation(jobConfig: JobConfig): Result[JobConfig] =
    writeJobConfig(jobConfig).flatMap{ config =>
      val errs = allPostValidations.flatMap(_(config.root()))
      Either.cond(errs.isEmpty, jobConfig, errs).toResult(errs => 
        "Data Quality job configuration post-validation failed with following errors:" +: errs.map(_.prettify).toVector
      )
    }

  /**
   * Validates Data Quality application configuration:
   * if configuration can be written to TypeSafe config object then it is valid.
   * @param config Instance of Data Quality application configuration
   * @return Either "Success" or a list of validation errors.
   */
  def validateAppConfig(config: AppConfig): Result[String] =
    writeAppConfig(config).map(_ => "Success")

  /**
   * Validates Data Quality job configuration:
   * if configuration can be written to TypeSafe config object 
   * and passes all post validation checks then it is valid.
   * @param config Instance of Data Quality job configuration
   * @return Either "Success" or a list of validation errors.
   */
  def validateJobConfig(config: JobConfig): Result[String] =
    runPostValidation(config).map(_ => "Success")

  /**
   * Reads and validates Data Quality application configuration.
   * Application configuration reading includes two stages:
   *   - Reading input into TypeSafe Config object
   *   - Parsing config object into instance of Data Quality application configuration class
   * Validations at each stage are chained i.e. if the above stage fails the next stages
   * are not evaluated. This is intentional behaviour: there is no point in parsing config object if
   * it wasn't successfully read from input at the first stage.
   * @param input Application configuration input
   * @param parser Implicit TypeSafe config parser for input of type T
   * @tparam T Type of input
   * @return Either a valid application configuration or a list of reading or validation errors
   */
  def readAppConfig[T](input: T)(implicit parser: ConfigParser[T]): Result[AppConfig] =
    read[T](input, "application").flatMap(parse[AppConfig](_, "application", "appConfig"))

  /**
   * Reads Data Quality job configuration.
   * Job configuration reading includes three stages:
   *   - Reading input into TypeSafe Config object
   *   - Parsing config object into instance of Data Quality job configuration class
   *   - Running post validations for Data Quality job configuration
   * Validations at each stage are chained i.e. if the above stage fails the next stages
   * are not evaluated. This is intentional behaviour: there is no point in parsing config object if
   * it wasn't successfully read from input at the first stage. And, again, there is no sense in running
   * post validations if Data Quality job configuration wasn't parsed successfully as the whole point of post validations
   * is to check cross reference validity of different objects in Data Quality job configuration.
   * @param input Job configuration input
   * @param parser Implicit TypeSafe config parser for input of type T
   * @tparam T Type of input
   * @return Either a valid job configuration or a list of reading or validation errors
   */
  def readJobConfig[T](input: T)(implicit parser: ConfigParser[T]): Result[JobConfig] =
    read[T](input, "job")
      .flatMap(parse[JobConfig](_, "job", "jobConfig"))
      .flatMap(runPostValidation)

  /**
   * Reads Encrypted Data Quality job configuration.
   * Job configuration reading includes three stages:
   *   - Reading input into TypeSafe Config object
   *   - Decryption of previously encrypted sensitive fields.
   *   - Parsing config object into instance of Data Quality job configuration class
   *   - Running post validations for Data Quality job configuration
   *     Validations at each stage are chained i.e. if the above stage fails the next stages
   *     are not evaluated. This is intentional behaviour: there is no point in parsing config object if
   *     it wasn't successfully read from input at the first stage. And, again, there is no sense in running
   *     post validations if Data Quality job configuration wasn't parsed successfully as the whole point of post validations
   *     is to check cross reference validity of different objects in Data Quality job configuration.
   *
   * @param input     Job configuration input
   * @param parser    Implicit TypeSafe config parser for input of type T
   * @param encryptor Implicit configuration encryptor
   * @tparam T Type of input
   * @return Either a valid job configuration or a list of reading or validation errors
   */
  def readEncryptedJobConfig[T](input: T)(implicit parser: ConfigParser[T],
                                          encryptor: ConfigEncryptor): Result[JobConfig] =
    read[T](input, "job")
      .flatMap(encryptor.decryptConfig)
      .flatMap(parse[JobConfig](_, "job", "jobConfig"))
      .flatMap(runPostValidation)
      
  
  object RenderOptions {
    val COMPACT: ConfigRenderOptions = 
      ConfigRenderOptions.defaults().setComments(false).setOriginComments(false).setFormatted(false)
    val FORMATTED: ConfigRenderOptions =
      ConfigRenderOptions.defaults().setComments(false).setOriginComments(false).setFormatted(true)
  }
}
