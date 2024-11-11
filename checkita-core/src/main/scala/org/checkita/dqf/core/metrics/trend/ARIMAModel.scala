package org.checkita.dqf.core.metrics.trend

import org.apache.commons.math3.optim.nonlinear.scalar.gradient.NonLinearConjugateGradientOptimizer
import org.apache.commons.math3.optim.nonlinear.scalar.{GoalType, ObjectiveFunction, ObjectiveFunctionGradient}
import org.apache.commons.math3.optim.{InitialGuess, MaxEval, MaxIter, SimpleValueChecker}
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression
import org.checkita.dqf.utils.Common.jsonFormats
import org.json4s.jackson.Serialization.write

import java.sql.Timestamp
import scala.annotation.tailrec

class ARIMAModel(p: Int, d: Int, q: Int) extends TrendMetricModel {

  /**
   * Addition to max lag for ARMA model initial parameter estimation by Hannan-Rissanen algorithm.
   */
  private val hannanRissanenMaxLagDelta = 3
  
  /**
   * Infer trend metric value using ARIMA model 
   * fitted over historical metric results
   *
   * @param data Historical metric results.
   * @param ts   UTC timestamp of current reference date.
   * @return Trend metric value and optional additional result 
   *         (may contain some information about model parameters)
   */
  override def infer(data: Seq[(Timestamp, Double)], 
                     ts: Timestamp): (Double, Option[String]) = {

    assert(Seq(p, d, q).forall(_ >= 0), "Negative parameters for ARIMA model are not allowed.")
    assert(p + q > 0, "Either AR (p) or MA (q) parameter of ARIMA model or both must be non-zero.")
    assert(
      data.size > 2 * (math.max(p, q) + hannanRissanenMaxLagDelta) + d + 1,
      "In order to fit ARIMA model, the size of historical data set must be greater than " +
        s"'2 * (max(p, q) + $hannanRissanenMaxLagDelta) + d + 1' of ARIMA order defined in trend metric: " +
        s"dataset contains ${data.size} metric results but minimum required size is " +
        2 * (math.max(p, q) + hannanRissanenMaxLagDelta) + d + 2
    )
    
    val model = new ARIMAOptimizer(data.sortBy(_._1.getTime).map(_._2), this.p, this.d, this.q).fit
    val modelInfo = Map(
      "No. Observations" -> model.noObs,
      "Log Likelihood" -> model.logLikelihood,
      "AIC (approximate)" -> model.approxAIC,
      "Sigma2" -> model.sigma,
      "Coefficients" -> Map(
        "Intercept" -> model.getIntercept,
        "AR Coefficients" -> model.getARCoefs,
        "MA Coefficients" -> model.getMACoefs
      )
    )
    
    model.forecast -> Some(write(modelInfo))
  }

  class ARIMAOptimizer(data: Seq[Double], p: Int, d: Int, q: Int) {
    private lazy val (diffedData, firstObs) = makeDiff(data, d)
    private lazy val diffedSiftedData: Seq[Seq[Double]] = shiftData(diffedData, p, keepOriginal = true)

    class ARIMA(val params: Seq[Double]) {
      private lazy val intercept: Double = params.head
      private lazy val arCoefs: Seq[Double] = params.slice(1, p + 1)
      private lazy val maCoefs: Seq[Double] = params.takeRight(q)

      /**
       * Forecasts next point.
       */
      def forecast: Double = {
        val forecastPoint = 0.0 +: getNextX(diffedData, p)
        val dataForecast = diffedSiftedData.drop(math.max(q - p, 0)) :+ forecastPoint
        val forecastY = generatePredictions(dataForecast).last
        inverseDiff(diffedData :+ forecastY, d, firstObs).last
      }
      
      def getIntercept: Double = intercept
      def getARCoefs: Seq[Double] = arCoefs
      def getMACoefs: Seq[Double] = maCoefs

      /**
       * Computes Sigma2
       */
      def sigma: Double = getCSS / diffedData.size
      
      /**
       * Computes log likelihood for current model parameters.
       * @return Log likelihood of the model
       */
      def logLikelihood: Double = {
        val css = getCSS
        val sigma = css / diffedData.size

        -0.5 * diffedData.size * math.log(2 * math.Pi * sigma) - css / (2 * sigma)
      }

      /**
       * Computes approximate AIC criterion for current model parameters
       * @return Approximate AIC.
       */
      def approxAIC: Double = -2 * this.logLikelihood + 2 * (p + q + 1)

      /**
       * Gets number of observations (size of initial time-series data)
       * @return Number of observations.
       */
      def noObs: Int = data.size

      /**
       * Yields predictions for train dataset.
       * Mainly used to compute residuals.
       * @return Fitted values for train dataset.
       */
      def fitted: Seq[Double] = this.generatePredictions(
        diffedSiftedData.drop(math.max(q - p, 0))
      )
      
      private def getCSS: Double = diffedData.drop(math.max(p, q))
        .zip(this.fitted)
        .map{ case (yTrue, yFit) => math.pow(yTrue - yFit, 2)}
        .sum
      
      /**
       * Yields prediction for provided data.
       * Data is a matrix where first column is true values 
       * and the rest ones are shifted AR terms.
       *
       * @return Predictions for train data.
       */
      private def generatePredictions(shiftedData: Seq[Seq[Double]]): Seq[Double] = {
        val (fitted, _) = shiftedData
          .foldLeft(Seq.empty[Double] -> Seq.fill[Double](q)(0)){ (acc, row) => 
            val (yTrue, arX) = row.head -> row.tail
            val (yFit, maX) = acc
            val arY = arX.zip(arCoefs).map(xc => xc._1 * xc._2).sum
            val maY = maX.zip(maCoefs).map(xc => xc._1 * xc._2).sum
            val y = intercept + arY + maY
            val e = yTrue - y
            (yFit :+ y, e +: maX.dropRight(1))
          }
        fitted
      }
    }

    /**
     * Fits ARIMA model to provided time-series data.
     * @return Trained ARIMA model.
     */
    def fit: ARIMA = {
      if (p > 0 && q == 0) {
        // ARIMA converges to simple AR model
        val (params, _) = fitARShifted(diffedSiftedData)
        new ARIMA(params) 
      } else {
        val initialGuess = new InitialGuess(initializeParameters.toArray)
        
        val optimizer = new NonLinearConjugateGradientOptimizer(
          NonLinearConjugateGradientOptimizer.Formula.FLETCHER_REEVES,
          new SimpleValueChecker(1e-7, 1e-7)
        )
        
        val objFunction = new ObjectiveFunction(
          (params: Array[Double]) => new ARIMA(params).logLikelihood
        )
        
        val gradientFunction = new ObjectiveFunctionGradient(
          (params: Array[Double]) => computeLLGradient(params).toArray
        )
        val optimalParams = optimizer.optimize(
          objFunction, gradientFunction, GoalType.MAXIMIZE, initialGuess, new MaxIter(100000), new MaxEval(100000)
        )
        new ARIMA(optimalParams.getPoint)
      }
    }
  
    /**
     * Gets initial estimation of ARIMA model parameters using Hannan-Rissanen algorithm.
     *
     * @return Initial estimate of ARIMA model parameters.
     */
    private def initializeParameters: Seq[Double] = {
      val maxLag = math.max(p, q) + hannanRissanenMaxLagDelta
      
      // estimate errors from AR(maxLag)
      val (_, errors) = fitAR(diffedData, maxLag)
      
      val shiftedErrors = shiftData(errors, q, keepOriginal = false).drop(math.max(p - q, 0))
      val shiftedData = shiftData(diffedData.drop(maxLag), p, keepOriginal = true).drop(math.max(q - p, 0))

      assert(
        shiftedData.size == shiftedErrors.size,
        "In order to estimate initial ARIMA parameters error terms and data terms must have the same number of rows."
      )

      val (params, _) = fitARShifted(
        shiftedData.zip(shiftedErrors).map { case (d, e) => d ++ e }
      )

      params
    }

    /**
     * Compute log likelihood gradient 
     * by applying small perturbations to each parameter of ARIMA model
     *
     * @return Log likelihood gradient vector.
     */
    private def computeLLGradient(params: Seq[Double]): Seq[Double] = {
      val delta = params.map(_.abs).min / 1e5
      
      val genDeltaLLs = (d: Double) => (0 to p + q).map { idx =>
        params.zipWithIndex.map {
          case (param, pos) => if (pos == idx) param + d else param
        }
      }.map(new ARIMA(_).logLikelihood)
      
      val plusDeltaLLs = genDeltaLLs(delta)
      val minusDeltaLLs = genDeltaLLs(-delta)
      
      plusDeltaLLs.zip(minusDeltaLLs).map{
        case (p, m) => (p - m) / 2 / delta
      }
    }
    
    /**
     * Trains auto-regression model provided with prepared data shifted to the desired lag indices.
     * First column must contain Y-values.
     *
     * @param trainData data to fit regression model
     * @return Regression model parameters and residuals.
     */
    private def fitARShifted(trainData: Seq[Seq[Double]]): (Array[Double], Array[Double]) = {
      val (arY, arX) = splitXY(trainData)
      if (arY.nonEmpty && arX.nonEmpty) {
        val linReg = new OLSMultipleLinearRegression()
        linReg.newSampleData(arY, arX)
        linReg.estimateRegressionParameters() -> linReg.estimateResiduals()
      } else Array.empty[Double] -> Array.empty[Double]
    }

    /**
     * Trains auto-regression model provided with time-series data and lag indices to train for.
     *
     * @param trainData Time-series train data.
     * @param maxLag    Maximum lag index to include in AR terms.
     * @return Regression model parameters and residuals
     */
    private def fitAR(trainData: Seq[Double], maxLag: Int): (Array[Double], Array[Double]) = {
      val shiftedData = shiftData(trainData, maxLag, keepOriginal = true)
      fitARShifted(shiftedData)
    }
    
    /**
     * Make differencing of data required number of times (order).
     *
     * @param data  Input time-series data
     * @param order Number of differencing operations to perform.
     * @return Diffed time-series data and first observations for each differencing order.
     * @note First observations are essential to restore time-series data.
     */
    @tailrec
    private def makeDiff(data: Seq[Double],
                         order: Int,
                         firstObs: Seq[Double] = Seq.empty): (Seq[Double], Seq[Double]) =
      if (order <= 0) data -> firstObs
      else {
        val diff = data.sliding(2).map(s => s.tail.head - s.head).toSeq
        makeDiff(diff, order - 1, data.head +: firstObs)
      }

    /**
     * Restores original time-series data from diffed one.
     * In order to restore time-series to its original state
     * it is essential to provide first observations points.
     *
     * @param data              Diffed time-series data.
     * @param order             Order of differencing to inverse
     * @param firstObservations Sequence of first observations at each level of differencing.
     *                          Order of observations in sequence must be reverted:
     *                          observation from last differencing must come first,
     *                          while first observation from original data should be at last position.
     * @return
     */
    @tailrec
    private def inverseDiff(data: Seq[Double],
                            order: Int,
                            firstObservations: Seq[Double]): Seq[Double] =
      if (order <= 0) data
      else {

        assert(
          firstObservations.size == order,
          "In order to inverse time-series differencing it is required to provide " +
            "first observations for each order of differencing. " +
            s"Provided ${firstObservations.size} observation, but order of differencing is $order."
        )

        val (curObs, restObs) = firstObservations.head -> firstObservations.tail
        val inverse = data.foldLeft(Vector(curObs))((v, d) => v :+ v.last + d)
        inverseDiff(inverse, order - 1, restObs)
      }

    /**
     * Crates training data set for AR model by shifting time-series data to the specified lags.
     *
     * @param data         Input time-series data.
     * @param maxLag       Maximum lag index to shift values for.
     * @param keepOriginal Boolean flag indicating whether original data is kept in the output array.
     * @return Array with data shifted to specified lags from 1 to maxLag.
     */
    private def shiftData(data: Seq[Double], maxLag: Int, keepOriginal: Boolean): Seq[Seq[Double]] = {
      if (maxLag == 0 && keepOriginal) data.map(Seq(_))
      else if (maxLag == 0) data.map(_ => Seq.empty[Double])
      else {
        val allLags = if (keepOriginal) 0 to maxLag else 1 to maxLag
        val window = maxLag + 1
        data
          .sliding(window)
          .map(_.toArray)
          .map { group =>
            allLags.map(lag => group(window - 1 - lag))
          }.toSeq
      }
    }

    /**
     * Retrieves X-vector from timeseries for next value forecasting by AR model.
     * Collects values for lag indices from 1 to maxLag.
     *
     * @param data Historical metric results.
     * @param maxLag Max lag index to retrieve values for.
     * @return X-vector for new value prediction.
     */
    def getNextX(data: Seq[Double], maxLag: Int): Seq[Double] =
      if (maxLag == 0) Seq.empty[Double]
      else (1 to maxLag).map(l => data(data.size - l))
    
    /**
     * Splits shifted data to Y vector and X-feature matrix.
     * Sequence is converted to array for input to linear regression model.
     *
     * @param data Shifted data to fir linear regression.
     * @return Y-vector and X-feature matrix.
     */
    private def splitXY(data: Seq[Seq[Double]]): (Array[Double], Array[Array[Double]]) = {
      if (data.forall(_.size <= 1)) Array.empty[Double] -> Array.empty[Array[Double]]
      else data.map(_.head).toArray -> data.map(_.tail.toArray).toArray
    }
  }
}
