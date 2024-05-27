package ru.raiffeisen.checkita.apps.stream

import ru.raiffeisen.checkita.apps.cli.CommandLineOptions
import ru.raiffeisen.checkita.context.{DQContext, DQStreamJob}
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.ResultUtils.Result

/**
 * Checkita Data Quality Streaming Application.
 */
object DataQualityStreamApp extends Logging {

  def main(args: Array[String]): Unit = {
    CommandLineOptions.parser().parse(args, CommandLineOptions("", Seq.empty)) match {
      case Some(opts) =>
        // create DQ Context
        val dqContext: Result[DQContext] = DQContext.build(
          opts.appConf, opts.refDate, opts.local, opts.shared, opts.migrate, opts.extraVars, opts.logLevel
        )
        // Build DQ Job
        val dqJob: Result[DQStreamJob] = dqContext.flatMap(_.buildStreamJob(opts.jobConf))
        // Run DQ Job
        val results: Result[Unit] = dqJob.flatMap(_.run())
        // Stop Spark Session in case if DQContext was successfully created.
        val _ = dqContext.flatMap(_.stop())
        // Log success or error message:
        results match {
          case Right(_) =>
            log.info("Checkita Data Quality streaming application stopped.")
          case Left(errs) =>
            log.error("Checkita Data Quality streaming application stopped with errors:")
            errs.foreach(log.error)
            sys.exit(1)
        }
      case None =>
        throw new IllegalArgumentException("Wrong application command line arguments provided.")
    }
  }
}
