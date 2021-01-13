
import org.apache.log4j.Logger
import dbest.ml.DBEstXGBoostRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.scalatest.funsuite.AnyFunSuite


class XGBoostRegressorTest extends AnyFunSuite {
  val logger = Logger.getLogger(this.getClass().getName())

  val spark = SparkSession.builder
    .master("local[*]")
    .getOrCreate

  ignore("XGBoostRegressor basic test") {
    val df = spark.sqlContext
      .range(0, 10000)
      .withColumn("uniform", F.rand)
      .withColumn("label", F.col("uniform") * 100)

    val features = Array("uniform")
    val label = "label"

    val dp = new dbest.dataprocessor.DataProcessor(df, features, label)
    val Array(split20, split80) = dp.processForRegression().getPreprocessedDF().randomSplit(Array(0.20, 0.80), 1800009193L)
    val testSet = split20.cache()
    val trainingSet = split80.cache()

    val xgboostRegressor = new DBEstXGBoostRegressor
    val xgbRegressionModel = xgboostRegressor.fit(trainingSet)
    
    val predictions = xgbRegressionModel.transform(testSet)
    predictions.select("label","prediction").show()

    val evaluator = new RegressionEvaluator()
      .setLabelCol(label)
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    logger.info("Root mean squared error: " + rmse)
    
    spark.close()
  }

  test("XGBoostRegressor crossValidate test") {
    val df = spark.sqlContext
      .range(0, 10000)
      .withColumn("uniform", F.rand)
      .withColumn("label", F.col("uniform") * 100)

    val features = Array("uniform")
    val label = "label"

    val dp = new dbest.dataprocessor.DataProcessor(df, features, label)
    val Array(split20, split80) = dp.processForRegression().getPreprocessedDF().randomSplit(Array(0.20, 0.80), 1800009193L)
    val testSet = split20.cache()
    val trainingSet = split80.cache()

    val xgboostRegressor = new DBEstXGBoostRegressor
    val xgbRegressionModel = xgboostRegressor.crossValidate(trainingSet, 2, 4)
    
    val predictions = xgbRegressionModel.transform(testSet)
    predictions.select("label","prediction").show()

    val evaluator = new RegressionEvaluator()
      .setLabelCol(label)
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    logger.info("Root mean squared error: " + rmse)
    
    spark.close()
  }
  
}