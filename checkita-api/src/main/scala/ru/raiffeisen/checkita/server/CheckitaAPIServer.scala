package ru.raiffeisen.checkita.server

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.comcast.ip4s._
import org.apache.logging.log4j.Level
import org.http4s.HttpRoutes
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.{Router, Server}
import ru.raiffeisen.checkita.appsettings.AppSettings
import ru.raiffeisen.checkita.config.Enums.DQStorageType
import ru.raiffeisen.checkita.dbmanager.APIJdbcStorageManager
import ru.raiffeisen.checkita.routes.StorageRoutes
import ru.raiffeisen.checkita.routes.ValidationRoutes.configValidationRoutes
import ru.raiffeisen.checkita.storage.Connections.DqStorageJdbcConnection
import ru.raiffeisen.checkita.utils.Common.getPrependVars
import ru.raiffeisen.checkita.utils.ResultUtils._

import scala.util.Try

object CheckitaAPIServer extends IOApp {

  /**
   * Read application configuration and builds an instance of application settings.
   *
   * @param appConfig Path to application configuration file
   * @param doMigration  Boolean flag indication whether DQ storage database migration
   *                     needs to be run prior API Server startup.
   * @param extraVariables User-defined map of extra variables to be prepended to application configuration
   *                       file prior its parsing.
   * @param logLvl Application logging level
   * @return Either an instance of application settings or a list of building errors.
   */
  private def readAppSettings(appConfig: String,
                              doMigration: Boolean,
                              extraVariables: Map[String, String],
                              logLvl: Level): Result[AppSettings] = {
    val prependVariables = getPrependVars(extraVariables)
    AppSettings.build(appConfig, None, false, false, doMigration, prependVariables, logLvl)
  }


  private def getStorageConnection(settings: AppSettings): Result[DqStorageJdbcConnection] =
    settings.storageConfig match {
      case Some(config) if !Seq(DQStorageType.File, DQStorageType.Hive).contains(config.dbType) =>
        liftToResult(new DqStorageJdbcConnection(config))
      case Some(config) => Left(Vector(
        s"Unable to create connection to DQ Storage for connection type of '${config.dbType.entryName}'. " +
        "Checkita API server support only JDBC-like connection types."
      ))
      case None => Left(Vector(
        "Unable to create connection to DQ Storage: connection configuration not found."
      ))
    }


  private def getStorageManager(dqStorageConn: DqStorageJdbcConnection): Result[APIJdbcStorageManager] =
    Try(new APIJdbcStorageManager(dqStorageConn)).toResult(
        preMsg = "Unable to connect to Data Quality storage due to following error:"
    )


  private def getRouter(dbManager: APIJdbcStorageManager, settings: AppSettings): HttpRoutes[IO] = Router(
    "/api/validation" -> configValidationRoutes,
    "/api/storage" -> StorageRoutes(dbManager, settings).dqStorageRoutes
  )

  private def getServer(router: HttpRoutes[IO]): Resource[IO, Server] = EmberServerBuilder
    .default[IO]
    .withHost(ipv4"0.0.0.0")
    .withPort(port"8080")
    .withHttpApp(router.orNotFound)
    .build

  override def run(args: List[String]): IO[ExitCode] = {
    CommandLineOptions.parser().parse(args, CommandLineOptions()) match {
      case Some(opts) =>
        val router = for {
          settings <- readAppSettings(opts.appConf, opts.migrate, opts.extraVars, opts.logLevel)
          dqStorageConn <- getStorageConnection(settings)
          dqStorageManager <- getStorageManager(dqStorageConn)
        } yield getRouter(dqStorageManager, settings)

        router match {
          case Right(rtr) =>
            println("Running API server...")
            getServer(rtr).use(_ => IO.never).as(ExitCode.Success)
          case Left(errs) => throw new IllegalArgumentException(
            s"Unable to startup Checkita API server due to following errors:\n${errs.mkString("\n")}"
          )
        }
      case None =>
        throw new IllegalArgumentException("Wrong application command line arguments provided.")
    }
  }
}
