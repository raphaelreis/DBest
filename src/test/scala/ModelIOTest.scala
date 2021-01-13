import dbest.ml._
import DataLoader._
import dbest.dataprocessor.DataProcessor
import java.io.File
import settings.Settings
import client.DBestClient
import java.nio.file.{Paths, Files}
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.ml.evaluation.RegressionEvaluator


class ModelIOTest extends AnyFunSuite {
    // Init settings and logger
    val appName = "ModelIO test"
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

    

    test("ModelIO test model writting/reading regressor") {
        // Initialization
        val spark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate()
        val df = spark.read
            .option("delimiter", "|")
            .csv(settings.baseDir + "data/store_sales_sample.dat")
            .withColumn("ss_wholesale_cost", F.col("_c11").cast(DoubleType))
            .withColumn("ss_list_price", F.col("_c12").cast(DoubleType))
            .select("ss_list_price", "ss_wholesale_cost")
            .na.drop()

        val features = Array("ss_list_price")
        val label = "ss_wholesale_cost"
        val dp = new DataProcessor(df, features, label)
        val Array(split20, split80) = dp.processForRegression().getPreprocessedDF().randomSplit(Array(0.20, 0.80), 1800009193L)
        val testSet = split20.cache()
        val trainingSet = split80.cache()
        
    // Training
        val model = new DBEstXGBoostRegressor()
        model.crossValidate(trainingSet, 2, 4)
    
    // Show Best model characteritics
        logger.info("Best model average metrics: " + model.cvModel.avgMetrics.mkString(","))
    
    // Evaluate prediction
        val predictions = model.transform(testSet)
        predictions.select(label,"prediction").show()

        val evaluator = new RegressionEvaluator()
            .setLabelCol(label)
            .setPredictionCol("prediction")
            .setMetricName("rmse")

        val rmse = evaluator.evaluate(predictions)
        logger.info("Root mean squared error: " + rmse)

    // Write model
        val modelIO = new ModelIO(settings.rpath, df, features, label, 1.0)
        modelIO.writeModel(model)

    // Read model
        val newModelIO = new ModelIO(settings.rpath, df, features, label, 1.0)
        val newModel = new DBEstXGBoostRegressor()
        newModelIO.readModel(newModel)
    
    // Verify that the prediction is the same as before writting
        val newPredictions = newModel.transform(testSet)
        newPredictions.select(label,"prediction").show()

        val newEvaluator = new RegressionEvaluator()
            .setLabelCol(label)
            .setPredictionCol("prediction")
            .setMetricName("rmse")

        val newRmse = newEvaluator.evaluate(newPredictions)
        logger.info("Root mean squared error: " + newRmse)

        assert(model.cvModel.avgMetrics.mkString(",") == newModel.cvModel.avgMetrics.mkString(","), "Average metrics are differents")
        assert(rmse == newRmse, "RMSE values are different")

        spark.close()
    }

    
}