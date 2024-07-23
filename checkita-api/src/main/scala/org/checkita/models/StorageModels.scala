package org.checkita.models

import org.checkita.storage.Models._

object StorageModels {

  /**
   * Object to store number of failed checks per given reference date.
   *
   * @param date      Job reference date
   * @param failCount Number of failed checks. -1 if no checks are found.
   */
  case class JobFailCount(date: String, failCount: Int)

  /**
   * Object to store job summary information which includes following:
   *   - job ID
   *   - job description (if available)
   *   - date of last job run (latest reference date)
   *   - sequence of job results (number of failed checks per run) within given time interval.
   *
   * @param jobId          Job ID
   * @param jobDescription Job description
   * @param lastRun        Latest reference date
   * @param results        Sequence of job results (number of failed checks per run).
   */
  case class JobInfo(jobId: String, 
                     jobDescription: String,
                     lastRun: String,
                     results: Seq[JobFailCount])

  /**
   * Object to store job results per given reference date.
   *
   * @param regularMetrics  Sequence of regular metric results
   * @param composedMetrics Sequence of composed metric results
   * @param loadChecks      Sequence of load check results
   * @param checks          Sequence of check results
   * @param errors          Sequence of collected metric errors.
   */
  case class JobResults(regularMetrics: Seq[ResultMetricRegular],
                        composedMetrics: Seq[ResultMetricComposed],
                        loadChecks: Seq[ResultCheckLoad],
                        checks: Seq[ResultCheck],
                        errors: Seq[ResultMetricError]) {
    /**
     * Checks if job results are empty, i.e. all type of results are empty.
     *
     * @return Boolean flag indicating whether job results are empty.
     */
    def isEmpty: Boolean = productIterator.map(_.asInstanceOf[Seq[Any]]).map(_.isEmpty).reduce(_ && _)
  }
  
}
