package dbest.ml

import traits.DBEstModel
import org.apache.log4j.Logger
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor

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
    val xgbRegressor  = new XGBoostRegressor(xgbParam)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumWorkers(numWorkers)

    val paramGrid = new ParamGridBuilder()
      .addGrid(xgbRegressor.maxDepth, Array(2, 4, 6))
      .addGrid(xgbRegressor.eta,  Array(0.3, 0.2, 0.1))
      .addGrid(xgbRegressor.numRound, Array(2, 3, 4))
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

