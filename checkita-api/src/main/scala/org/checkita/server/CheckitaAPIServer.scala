package org.checkita.server

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.comcast.ip4s._
import org.apache.logging.log4j.Level
import org.checkita.dbmanager.APIJdbcStorageManager
import org.checkita.routes.{StorageRoutes, ValidationRoutes}
import org.http4s.HttpRoutes
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.{Router, Server}
import org.checkita.appsettings.AppSettings
import org.checkita.config.Enums.DQStorageType
import org.checkita.storage.Connections.DqStorageJdbcConnection
import org.checkita.utils.Common.getPrependVars
import org.checkita.utils.Logging
import org.checkita.utils.ResultUtils._

import scala.util.Try

object CheckitaAPIServer extends IOApp with Logging {

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
    AppSettings.build(
      appConfig, None, isLocal = false, isShared = false, doMigration = doMigration, prependVariables, logLvl
    )
  }
  /**
   * Retrieves connection to DQ Storage.
   * @param settings Application settings
   * @return Either an instance of DQ Storage connection or a list of connection errors.
   */
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

  /**
   * Builds DQ Storage manager provided with DQ Storage connection.
   * @param dqStorageConn DQ Storage connection (jdbc-like).
   * @return Either an instance of DQ Storage manager or a list of building errors.
   */
  private def getStorageManager(dqStorageConn: DqStorageJdbcConnection): Result[APIJdbcStorageManager] =
    Try(new APIJdbcStorageManager(dqStorageConn)).toResult(
        preMsg = "Unable to connect to Data Quality storage due to following error:"
    )

  /**
   * Assembles API routes.
   * @param dbManager DQ Storage manager
   * @param settings Application settings
   * @return All http routes for Checkita API.
   */
  private def getRouter(dbManager: APIJdbcStorageManager, settings: AppSettings): HttpRoutes[IO] = Router(
    "/api/validation" -> ValidationRoutes.configValidationRoutes,
    "/api/storage" -> StorageRoutes(dbManager, settings).dqStorageRoutes
  )

  /**
   * Builds HTTP Server provided with API routes.
   * @param router API routes.
   * @return Instance of HTTP server.
   */
  private def getServer(router: HttpRoutes[IO]): Resource[IO, Server] = EmberServerBuilder
    .default[IO]
    .withHost(ipv4"0.0.0.0")
    .withPort(port"8080")
    .withHttpApp(router.orNotFound)
    .build

  /**
   * Runs the HTTP server.
   */
  override def run(args: List[String]): IO[ExitCode] = {
    CommandLineOptions.parser().parse(args, CommandLineOptions()) match {
      case Some(opts) =>
        val router = for {
          settings <- readAppSettings(opts.appConf, opts.migrate, opts.extraVars, opts.logLevel)
          _ <- Try(initLogger(settings.loggingLevel)).toResult()
          dqStorageConn <- getStorageConnection(settings)
          dqStorageManager <- getStorageManager(dqStorageConn)
        } yield getRouter(dqStorageManager, settings)

        router match {
          case Right(r) =>
            log.info("Running API server...")
            getServer(r).use(_ => IO.never).as(ExitCode.Success)
          case Left(errs) => throw new IllegalArgumentException(
            s"Unable to startup Checkita API server due to following errors:\n${errs.mkString("\n")}"
          )
        }
      case None =>
        throw new IllegalArgumentException("Wrong application command line arguments provided.")
    }
  }
}
