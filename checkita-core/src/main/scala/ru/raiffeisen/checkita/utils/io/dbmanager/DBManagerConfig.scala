package ru.raiffeisen.checkita.utils.io.dbmanager

import com.typesafe.config.Config
import scala.util.Try

case class DBManagerConfig(
                            id: String,
                            subtype: String,
                            host: Option[String],
                            path: Option[String] = None,
                            port: Option[String] = None,
                            service: Option[String] = None,
                            user: Option[String] = None,
                            password: Option[String] = None,
                            schema: Option[String] = None
                          ) {

  def this(config: Config) = {
    this(
      Try(config.getString("id")).getOrElse(""),
      config.getString("subtype"),
      Try(config.getString("host")).toOption,
      Try(config.getString("path")).toOption,
      Try(config.getString("port")).toOption,
      Try(config.getString("service")).toOption,
      Try(config.getString("user")).toOption,
      Try(config.getString("password")).toOption,
      Try(config.getString("schema")).toOption
    )
  }
}
