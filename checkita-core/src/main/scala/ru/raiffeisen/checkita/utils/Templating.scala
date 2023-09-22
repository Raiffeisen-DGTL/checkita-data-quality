package ru.raiffeisen.checkita.utils

import com.github.mustachejava.DefaultMustacheFactory

import java.io.{StringReader, StringWriter}
import scala.collection.JavaConverters._
import scala.util.Try

object Templating {
  /**
   * Renders mustache template
   * @param template Template to render
   * @param values Values map for substitution
   * @return Rendered template
   */
  def renderTemplate(template: String, values: Map[String, String]): Option[String] = Try{
    val factory = new DefaultMustacheFactory()
    val mustache = factory.compile(new StringReader(template), "template")
    val output = new StringWriter
    mustache.execute(output, values.asJava)
    output.toString
  }.toOption
}

