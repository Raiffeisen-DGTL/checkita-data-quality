package org.checkita.apps.batch

import org.checkita.apps.cli.CommandLineOptions
import org.checkita.context.{DQContext, DQBatchJob}
import org.checkita.storage.Models.ResultSet
import org.checkita.utils.Logging
import org.checkita.utils.ResultUtils.Result

/**
 * Checkita Data Quality Batch Application.
 */
object DataQualityBatchApp extends Logging {

  def main(args: Array[String]): Unit = {
    CommandLineOptions.parser().parse(args, CommandLineOptions("", Seq.empty)) match {
      case Some(opts) =>
        // create DQ Context
        val dqContext: Result[DQContext] = DQContext.build(
          opts.appConf, opts.refDate, opts.local, opts.shared, opts.migrate, opts.extraVars, opts.logLevel
        )
        // Build DQ Job
        val dqJob: Result[DQBatchJob] = dqContext.flatMap(_.buildBatchJob(opts.jobConf))
        // Run DQ Job
        val results: Result[ResultSet] = dqJob.flatMap(_.run)
        // Stop Spark Session in case if DQContext was successfully created.
        val _ = dqContext.flatMap(_.stop())
        // Log success or error message:
        results match {
          case Right(_) =>
            log.info("Checkita Data Quality batch application completed successfully.")
          case Left(errs) =>
            log.error("Checkita Data Quality batch application finished with following errors:")
            errs.foreach(log.error)
            sys.exit(1)
        }
      case None =>
        throw new IllegalArgumentException("Wrong application command line arguments provided.")
    }
  }
}