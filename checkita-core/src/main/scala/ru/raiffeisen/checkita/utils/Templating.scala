package ru.raiffeisen.checkita.utils

import com.github.mustachejava.codes.ValueCode
import com.github.mustachejava.{DefaultMustacheFactory, Mustache, MustacheException}

import java.io.{StringReader, StringWriter, Writer}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object Templating {

  /**
   * Custom Mustache Factory where encode method is overridden to suppress html escaping. 
   */
  private class NonEscapeMustacheFactory extends DefaultMustacheFactory {
    override def encode(value: String, writer: Writer): Unit = writer.write(value)
  }

  /**
   * Compiles mustache template using NonEscapeMustacheFactory
   * @param template Mustache template to compile
   * @return Compiled mustache template
   */
  private def compileTemplate(template: String): Mustache = {
    val factory = new NonEscapeMustacheFactory()
    factory.compile(new StringReader(template), "template")
  }
  
  /**
   * Function to render Mustache templates.
   * @param template Mustache template to render
   * @param values Map of (paramName -> paramValue) to use for substitution.
   * @return Rendered template
   */
  def renderTemplate(template: String, values: Map[String, String]): String = Try{
    val mustache = compileTemplate(template)
    val output = new StringWriter
    mustache.execute(output, values.asJava)
    output.toString
  } match {
    case Success(s) => s
    case Failure(e) => 
      throw new MustacheException("Failed to render template with following error:\n" + e.getMessage)
  }

  /**
   * Gets first-level value tokens from mustache template
   * @param template Mustache template to get tokens from
   * @return Sequence of value tokens
   * @note It is assumed that templates used in Data Quality contain only value tokens.
   */
  def getTokens(template: String): Seq[String] = Try{
    val mustache = compileTemplate(template)
    mustache.getCodes.toList.filter(_.isInstanceOf[ValueCode]).map(_.getName)
  } match {
    case Success(tokens) => tokens
    case Failure(e) => 
      throw new MustacheException("Failed to retrieve tokens from template with following error:\n" + e.getMessage)
  }
}
