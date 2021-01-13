package dbest.ml

import org.apache.log4j.Logger
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator

import traits.DBEstModel

class DBEstXGBoostRegressor extends DBEstRegressor with DBEstModel {
  private val logger = Logger.getLogger(this.getClass().getName())
  val name = "xgboost_regressor"
  
  val xgbParam = Map("eta" -> 0.3,
      "max_depth" -> 6,
      "objective" -> "reg:squarederror",
      "num_round" -> 10,
      "num_workers" -> 2)

  def fit(traingSet: DataFrame): PipelineModel = {
    val xgbRegressor  = new XGBoostRegressor(xgbParam)
      .setFeaturesCol("features")
      .setLabelCol("label")
    
    val estimator = new Pipeline()
      .setStages(Array(xgbRegressor))

    estimator.fit(traingSet)
  }

  def crossValidate(trainingSet: DataFrame, numFolds: Int, numWorkers: Int) = {
    logger.info("Crossvalidation of LinearRegressor Started...")
    val xgbRegressor  = new XGBoostRegressor(xgbParam)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumWorkers(numWorkers)

    val paramGrid = new ParamGridBuilder()
      .addGrid(xgbRegressor.maxDepth, Array(2, 4))
      .addGrid(xgbRegressor.eta,  Array(0.3, 0.2))
      .addGrid(xgbRegressor.numRound, Array(2, 3))
      .build()
 
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val cv = new CrossValidator()
      .setEstimator(xgbRegressor)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)

    cvModel = cv.fit(trainingSet)
    cvModel
  }
 }

