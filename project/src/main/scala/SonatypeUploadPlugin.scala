package src.main.scala

import org.apache.http.client.ResponseHandler
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.RequestBuilder
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpException, HttpHeaders, HttpResponse}
import org.json4s._
import org.json4s.native.JsonMethods._
import sbt.Keys._
import sbt._
import sbt.internal.util.ManagedLogger
import sbt.plugins.JvmPlugin

import java.io.{FileInputStream, FileOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Base64
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object SonatypeUploadPlugin extends AutoPlugin {
  object autoImport {
    // settings:
    val sonatypeStagingDirectory = settingKey[File]("The local directory where signed package will be published")
    val sonatypeStagingResolver = settingKey[Option[Resolver]]("The local resolver for signed publishing")
    val sonatypeBundleDirectory = settingKey[File]("The local directory to put zipped bundle for uploading.")
    val sonatypeDeploymentName = settingKey[String]("Deployment name. Default is <organization>.<artifact_name>-<version>")
    
    
    // commands:
    val sonatypeUpload = taskKey[Unit]("Upload bundle to Sonatype Central")
    val sonatypeClean  = taskKey[Unit]("Clean up staging and bundle directories")
    val printProjectFullName = taskKey[Unit]("Prints current project name and version and returns it as task output value")
  }

  import autoImport._

  // make sure it triggers automatically
  override def trigger = AllRequirements
  override def requires = JvmPlugin
  
  class SonatypeService(log: ManagedLogger) {
    implicit val formats: Formats = DefaultFormats
    
    private val AUTO = "AUTOMATIC"
    private val MANAGED = "USER_MANAGED"
    
    private val MAX_TRIES = 60
    private val POKE_INTERVAL = 30000
    
    private object Endpoints {
      private val base_url = "https://central.sonatype.com/api/v1/publisher"
      
      val UPLOAD: String = s"$base_url/upload"
      val STATUS: String = s"$base_url/status"
    }
    
    private lazy val client = HttpClientBuilder.create().setDefaultRequestConfig(
      RequestConfig.custom().setConnectTimeout(60 * 1000).build()
    ).build()
    
    private lazy val SONATYPE_TOKEN: String = {
      val sonaUser = sys.props.get("SONATYPE_USERNAME")
        .orElse(sys.env.get("SONATYPE_USERNAME"))
        .getOrElse(throw new IOException("SONATYPE_USERNAME variable is not found."))

      val sonaPass = sys.props.get("SONATYPE_PASSWORD")
        .orElse(sys.env.get("SONATYPE_PASSWORD"))
        .getOrElse(throw new IOException("SONATYPE_PASSWORD variable is not found."))

      Base64.getEncoder.encodeToString(s"$sonaUser:$sonaPass".getBytes(StandardCharsets.UTF_8))
    }
  
    private trait SonatypeResponseHandler[T] extends ResponseHandler[Try[T]] {
      protected def decode(content: HttpEntity): Try[T]
      
      override def handleResponse(response: HttpResponse): Try[T] = {
        val statusCode = response.getStatusLine.getStatusCode
        val statusMsg = response.getStatusLine.getReasonPhrase
        if (statusCode >= 200 && statusCode <= 300) decode(response.getEntity)
        else Failure(new HttpException(
          s"Failed request with status $statusCode and following message: $statusMsg.\n" +
            s"Response text: ${response.getEntity.toString}"
        ))
      }
    }
    
    private lazy val stringResponseHandler: ResponseHandler[Try[String]] = 
      new SonatypeResponseHandler[String] {
        override protected def decode(content: HttpEntity): Try[String] = Try(EntityUtils.toString(content))
      }
      
    private lazy val statusResponseHandler: ResponseHandler[Try[String]] =
      new SonatypeResponseHandler[String] {
        override protected def decode(content: HttpEntity): Try[String] =
          Try(EntityUtils.toString(content))
            .map(js => parse(js).extract[Map[String, Any]])
            .map(m => m("deploymentState").asInstanceOf[String])
      }
    
    private def checkStatus(deploymentStatus: String): Boolean = 
      if (deploymentStatus.toUpperCase == "FAILED") throw new IOException(
        "Deployment validation failed!"
      ) else {
        log.info(s"Deployment current status is: $deploymentStatus")
        !Seq("PENDING", "VALIDATING").contains(deploymentStatus)
      }
    
    final def uploadBundle(bundle: java.io.File, deploymentName: String): Try[String] = {
      log.info("Uploading bundle to Sonatype Central.")
      log.info(s"Bundle to upload: name=$deploymentName, location=${bundle.getPath}")
      
      val entity = MultipartEntityBuilder.create().addBinaryBody(
        "bundle", bundle, ContentType.MULTIPART_FORM_DATA, bundle.getName
      ).build()
      
      val request = RequestBuilder.post()
        .setUri(Endpoints.UPLOAD)
        .addParameter("name", deploymentName)
        .addParameter("publishingType", MANAGED)
        .setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $SONATYPE_TOKEN")
        .setEntity(entity)

      client.execute(request.build(), stringResponseHandler)
        .map{id => log.info(s"Bundle uploaded. DeploymentID = $id"); id}
    }
    
    @tailrec
    final def checkUploadStatus(deploymentID: String, attempt: Int = 0): Unit = {
      if (attempt >= MAX_TRIES) throw new IOException(
        "Failed to verify deployment status: reached maximum number of attempts."
      )
      log.info(s"Checking status of deployment. Attempt #$attempt")

      val request = RequestBuilder.post()
        .setUri(Endpoints.STATUS)
        .addParameter("id", deploymentID)
        .setHeader(HttpHeaders.AUTHORIZATION, s"Bearer $SONATYPE_TOKEN")

      val status = client.execute(request.build(), statusResponseHandler).map(checkStatus)
      
      status match {
        case Success(s) => 
          if (s) log.success(s"Deployment validation successfully completed!")
          else {
            log.info(s"Deployment validation hasn't completed yet. Will try again in ${POKE_INTERVAL / 1000} seconds.")
            Thread.sleep(POKE_INTERVAL)
            checkUploadStatus(deploymentID, attempt + 1)
          }
        case Failure(e) => throw e
      }
    }
  }

  private def zipBundle(stagingPath: String, bundlePath: String): Try[File] = for {
    bundleDir <- Try(Files.createDirectory(Paths.get(bundlePath)))
    bundleFile <- zipDirectory(stagingPath, bundleDir.toFile.getPath)
  } yield bundleFile
  
  def zipDirectory(sourceDir: String, outputDir: String): Try[File] = Try{
    val source = new java.io.File(sourceDir)
    val outputFile = s"$outputDir/bundle.zip"
    val fileOutStream = new FileOutputStream(outputFile)
    val zipOutStream = new ZipOutputStream(fileOutStream)
    
    zip(List((new java.io.File(sourceDir), "", true)), zipOutStream)

    zipOutStream.close()
    fileOutStream.close()
    new java.io.File(outputFile)
  }
  
  @tailrec
  def zip(files: List[(java.io.File, String, Boolean)], zipOut: ZipOutputStream): Unit = 
    files match {
      case Nil => ()
      case head :: rest => 
        if (head._1.isHidden) throw new IOException(s"Attempting to zip hidden file ${head._1.getPath}")
        if (head._1.isDirectory) {
          if (!head._3) {
            val entryName = if (head._2.endsWith("/")) head._2 else head._2 + "/"
            zipOut.putNextEntry(new ZipEntry(entryName))
            zipOut.closeEntry()
          }
          val dirPath = if (head._3) "" else head._2 + "/"
          val children = head._1.listFiles().map(f => (f, dirPath + f.getName, false)).toList
          zip(children ++ rest, zipOut)
        } else {
          val fileStream = new FileInputStream(head._1)
          val zipEntry = new ZipEntry(head._2)
          zipOut.putNextEntry(zipEntry)
          val bytes = new Array[Byte](1024)
          var length = 0
          while ({ length = fileStream.read(bytes); length } >= 0) {
            zipOut.write(bytes, 0, length)
          }
          fileStream.close()
          zip(rest, zipOut)
        }
    }
  
  private val sonatypeUploadTask: Def.Initialize[Task[Unit]] = Def.task{
    val service = new SonatypeService(streams.value.log)
    zipBundle(sonatypeStagingDirectory.value.getPath, sonatypeBundleDirectory.value.getPath)
      .flatMap(service.uploadBundle(_, sonatypeDeploymentName.value))
      .map(service.checkUploadStatus(_)).get
  }
  
  private val sonatypeCleanTask: Def.Initialize[Task[Unit]] = Def.task{
    val log = streams.value.log
    Seq(sonatypeStagingDirectory.value, sonatypeBundleDirectory.value).foreach { dir =>
      log.info(s"Removing directory: ${dir.getPath}")
      if (dir.exists()) {
        IO.delete(dir)
        log.info("Directory successfully removed.")
      } else {
        log.info("Directory does not exists. Nothing to delete.")
      }
    }
  }
  
  override def projectSettings: Seq[Setting[_]] = Seq(
    sonatypeStagingDirectory := {
      (ThisBuild / baseDirectory).value / "target" / "sonatype-staging" / s"${(ThisBuild / version).value}"
    },
    sonatypeStagingResolver := Some(Resolver.file("sonatype-staging-resolver", sonatypeStagingDirectory.value)),
    sonatypeBundleDirectory := {
      (ThisBuild / baseDirectory).value / "target" / "sonatype-staging" / s"${(ThisBuild / version).value}-bundle"
    },
    sonatypeDeploymentName := s"${organization.value}.${name.value}-${version.value}",
    sonatypeUpload := sonatypeUploadTask.value,
    sonatypeClean := sonatypeCleanTask.value
  )
}