package org.checkita.dqf.core.metrics.trend
import org.apache.commons.math3.stat.regression.SimpleRegression
import org.checkita.dqf.utils.Common.jsonFormats
import org.json4s.jackson.Serialization.write

import java.sql.Timestamp

/**
 * Simple linear regression model for trend metrics
 */
object LinearRegressionModel extends TrendMetricModel {

  /**
   * Infer trend metric value using simple linear regression 
   * fitted over historical metric results
   * 
   * @param data Historical metric results.
   * @param ts   UTC timestamp of current reference date.
   * @return Trend metric value
   *         
   * @note Timestamps are converted to epoch milliseconds.
   */
  override def infer(data: Seq[(Timestamp, Double)], ts: Timestamp): (Double, Option[String]) = {

    assert(
      data.size > 1,
      "In order to fit linear regression model, the historical data set must contain at least 2 results, " +
        s"but dataset contains only ${data.size} metric results."
    )
    
    val linReg = new SimpleRegression()

    data.foreach{ d =>
      linReg.addData(d._1.toInstant.toEpochMilli, d._2)
    }
    
    val forecast = linReg.predict(ts.toInstant.toEpochMilli)
    val info = Map(
      "No. Observations" -> linReg.getN,
      "MSE" -> linReg.getSumSquaredErrors / linReg.getN
    )

    forecast -> Some(write(info))
  }
}
