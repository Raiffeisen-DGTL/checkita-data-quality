package org.checkita.dqf.connections.schemaregistry

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema
import org.checkita.dqf.config.jobconf.Schemas.RegistrySchemaConfig
import org.checkita.dqf.utils.Common.paramsSeqToMap

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

/**
 * Schema registry connection used to obtain AVRO schemas from Confluent Kafka Schema Registry.
 * given either their ID or a subject. Also possible to fetch schema with explicit version.
 * By default latest version is obtained.
 *
 * @note At this point only AVRO schemas are supported.
 * @note This connection is not used to read any data, therefore, it does not extend DQConnection.
 * @param config Registry-kind schema configuration
 */
case class SchemaRegistryConnection(config: RegistrySchemaConfig) {
  
  private val cacheCapacity: Int = 10

  /**
   * Implicit conversion used to wrap map of properties into an option.
   *
   * @param value Properties to wrap into an option.
   */
  implicit class MapOps(value: Map[String, String]) {
    def toOptionMap: Option[Map[String, String]] =
      Some(value).flatMap(v => if (v.isEmpty) None else Some(v))
  }

  /**
   * Builds Confluent Kafka Schema Registry Client.
   * Users can provide additional connection properties as well as html headers.
   * Thus, different client builders are used depending on the input. 
   * 
   * @return Schema registry client instance
   */
  private def getClient: CachedSchemaRegistryClient = {
    val baseUrls = config.baseUrls.value.map(_.value).asJava
    val connProps = paramsSeqToMap(config.properties.map(_.value)).toOptionMap
    val connHeaders = paramsSeqToMap(config.headers.map(_.value)).toOptionMap
    val nullProps: java.util.Map[String, Any] = null 
    (connProps, connHeaders) match {
      case (Some(props), Some(headers)) => 
        new CachedSchemaRegistryClient(baseUrls, cacheCapacity, props.asJava, headers.asJava)
      case (Some(props), None) => new CachedSchemaRegistryClient(baseUrls, cacheCapacity, props.asJava)
      case (None, Some(headers)) => 
        new CachedSchemaRegistryClient(baseUrls, cacheCapacity, nullProps, headers.asJava)
      case _ => new CachedSchemaRegistryClient(baseUrls, cacheCapacity)
    }
  }

  /**
   * Confluent Kafka Schema Registry Client
   *
   * @note Make it lazy so the client won't be build until used.
   */
  private lazy val client = getClient

  /**
   * Parses schema obtained from Schema registry and returns its canonical string representation.
   *
   * @param schema Schema to parse.
   * @return Canonical string representation of the schema.
   * @note At this point only AVRO schemas are supported.
   */
  private def getCanonicalString(schema: Schema): String = { 
    val schemaType = schema.getSchemaType
    val schemaRef = s"schema (id = ${schema.getId}, subject = ${schema.getSubject}, version = ${schema.getVersion})"
    if (schemaType == null || schemaType == AvroSchema.TYPE) {
      client.parseSchema(schema).toScala.getOrElse(throw new IllegalArgumentException(
        s"Unable to parse registry $schemaRef."
      )).canonicalString()
    } else throw new IllegalArgumentException(s"Unsupported schema type of '$schemaType' for $schemaRef.")
  }

  /**
   * Get schema from registry by schema subject and version.
   *
   * @param subject Schema subject
   * @param version Schema version
   * @return Schema object.
   */
  private def getSchema(subject: String, version: Int): Schema = client.getByVersion(subject, version, false)

  /**
   * Get all schema versions by its ID.
   *
   * @param id Schema ID
   * @return Sequence of tuples (subject, version)
   */
  private def getAllVersionsById(id: Int): Seq[(String, Int)] = 
    client.getAllVersionsById(id).asScala.map(sv => (sv.getSubject, sv.getVersion.toInt)).toSeq

  /**
   * Get all schema versions by its subject.
   *
   * @param subject Schema subject
   * @return Sequence of tuples (subject, version)
   */
  private def getAllVersionsBySubject(subject: String): Seq[(String, Int)] =
    client.getAllVersions(subject).asScala.toSeq.map(v => (subject, v.toInt))

  /**
   * Gets schema version to read: 
   *   - if explicit version is provided, then checks for its presence in registry and returns this version.
   *   - if explicit version is not provided, then returns latest (maximum) available version.
   *
   * @param allVersions Sequence of all versions for schema of interest.
   * @param version     Optional explicit version.
   * @return Tuple of (subject, version) to be read.
   */
  private def getVersionToRead(allVersions: Seq[(String, Int)], version: Option[Int]): (String, Int) = 
    version match {
      case Some(ver) => 
        val versionFiltered = allVersions.filter(_._2 == ver)
        if (versionFiltered.nonEmpty) versionFiltered.head
        else throw new IllegalArgumentException(
          s"Schema with subject ${allVersions.head._1} does not have version $ver."
        )
      case None => allVersions.maxBy(_._2)
    }

  /**
   * Reads schema from registry provided by its ID and optional explicit version.
   *
   * @param id      Schema ID
   * @param version Optional schema version
   * @return Schema canonical string representation
   */
  def getSchemaById(id: Int, version: Option[Int]): String = {
    val (subj, ver) = getVersionToRead(
      getAllVersionsById(id),
      version
    )
    getCanonicalString(getSchema(subj, ver))
  }

  /**
   * Reads schema from registry provided by its subject and optional explicit version.
   *
   * @param subject Schema subject
   * @param version Optional schema version
   * @return Schema canonical string representation
   */
  def getSchemaBySubject(subject: String, version: Option[Int]): String = {
    val (subj, ver) = getVersionToRead(
      getAllVersionsBySubject(subject),
      version
    )
    getCanonicalString(getSchema(subj, ver))
  }
}
