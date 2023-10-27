package ru.raiffeisen.checkita.apps.batch

import ru.raiffeisen.checkita.apps.cli.CommandLineOptions
import ru.raiffeisen.checkita.context.{DQContext, DQJob}
import ru.raiffeisen.checkita.storage.Models.ResultSet
import ru.raiffeisen.checkita.utils.Logging
import ru.raiffeisen.checkita.utils.ResultUtils.Result

/**
 * Checkita Data Quality Batch Application.
 */
object DataQualityBatchApp extends App with Logging {
  
  CommandLineOptions.parser().parse(args, CommandLineOptions("", Seq.empty)) match {
    case Some(opts) => 
      // create DQ Context
      val dqContext: Result[DQContext] = DQContext.build(
        opts.appConf, opts.refDate, opts.local, opts.shared, opts.migrate, opts.extraVars, opts.logLevel
      )
      // Build DQ Job
      val dqJob: Result[DQJob] = dqContext.flatMap(_.buildJob(opts.jobConf))
      // Run DQ Job
      val results: Result[ResultSet] = dqJob.flatMap(_.run)
      // Stop Spark Session in case if DQContext was successfully created.
      val _ = dqContext.flatMap(_.stop())
      // Log success or error message:
      results match {
        case Right(_) =>
          log.info("Checkita Data Quality application completed successfully.")
        case Left(errs) =>
          log.error("Checkita Data Quality application finished with following errors:")
          errs.foreach(log.error)
          sys.exit(1)
      }
    case None =>
      throw new IllegalArgumentException("Wrong application command line arguments provided.")
  }
}