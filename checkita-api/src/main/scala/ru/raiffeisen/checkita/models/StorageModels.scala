package ru.raiffeisen.checkita.models

object StorageModels {
  
  case class JobFailCount(date: String, failCount: Int)
  case class JobInfo(jobId: String, 
                     jobDescription: String,
                     lastRun: String,
                     results: Seq[JobFailCount])
  
}
