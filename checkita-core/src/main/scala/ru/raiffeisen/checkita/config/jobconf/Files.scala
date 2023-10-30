package ru.raiffeisen.checkita.config.jobconf

import eu.timepit.refined.types.string.NonEmptyString
import ru.raiffeisen.checkita.config.RefinedTypes.URI

/**
 * @note General note on working with files in Checkita Framework:
 *       - Path may contain file system connector prefix such as 
 *         `file://` to read from local file system or `s3a://` to read from S3 storage.
 *       - It is up to user to setup all required spark configuration parameters to read from
 *         and write into specified file system.
 *       - If file system connector prefix is not defined then files are always 
 *         read from and written into Spark's default file system.
 *       - Pay attention when running framework in local mode:
 *         in this case spark will read files from local file system only.
 */
object Files {

  /**
   * Base trait for all types of configurations that refer to a file.
   * All such configurations must contain a path to a file.
   */
  trait FileConfig {
    val path: URI
  }

  /**
   * Base trait for ORC files
   */
  trait OrcFileConfig extends FileConfig

  /**
   * Base trait for Parquet files
   */
  trait ParquetFileConfig extends FileConfig

  /**
   * Base trait for Avro files. Configuration for avro files may contain reference to avro schema ID.
   */
  trait AvroFileConfig extends FileConfig {
    val schema: Option[NonEmptyString]
  }

  /**
   * Base trait for delimited text files such as CSV or TSV.
   * For such file configuration must contain following parameters:
   *   - delimiter symbol
   *   - quote symbol
   *   - escape symbol
   *   - flag indicating that schema must be read from header
   *   - explicit schema ID for cases when header is absent or should be ignored
   */
  trait DelimitedFileConfig extends FileConfig {
    val delimiter: NonEmptyString
    val quote: NonEmptyString
    val escape: NonEmptyString
    val header: Boolean
    val schema: Option[NonEmptyString]
  }

  /**
   * Base trait for fixed-width text files.
   * For such files configuration must contain reference explicit fixed (full or short) schema ID
   */
  trait FixedFileConfig extends FileConfig {
    val schema: NonEmptyString
  }
}
