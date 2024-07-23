package org.checkita.config.appconf

import eu.timepit.refined.types.string.NonEmptyString
import org.checkita.config.RefinedTypes.URI
import org.checkita.config.Enums.DQStorageType

/**
 * Application-level configuration describing connection to history database.
 *
 * @param dbType              Type of database used to store DQ data (one of the supported RDBMS)
 * @param url                 Connection URL (without protocol identifiers)
 * @param username            Username to connect to database with (if required)
 * @param password            Password to connect to database with (if required)
 * @param schema              Schema where data quality tables are located (if required)
 * @param saveErrorsToStorage Enables metric errors to be stored in storage database.
 *                            Be careful when storing metric errors in storage database
 *                            as this might overload the storage.
 */
final case class StorageConfig(
                                dbType: DQStorageType,
                                url: URI,
                                username: Option[NonEmptyString],
                                password: Option[NonEmptyString],
                                schema: Option[NonEmptyString],
                                saveErrorsToStorage: Boolean = false
                              )

