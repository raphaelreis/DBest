import org.scalatest.funsuite.AnyFunSuite
import org.apache.log4j.{Level, Logger}
import dbest.ml.LinearRegressor
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{rand, randn}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}

class LinearRegressorTest extends AnyFunSuite {
  val logger = Logger.getLogger(this.getClass().getName())

  val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("LinearRegressorTest")
        .getOrCreate() 

  test("Test LinearRegressor fit method") {
    val idx = spark.sqlContext.range(0, 100)
    val randomDf = idx.select("id").withColumn("uniform1", rand(42)).withColumn("uniform2", rand(42)).withColumn("uniform3", rand(42))
          
    val features = Array("uniform1", "uniform2")
    val label = "uniform3"

    val lr = new LinearRegressor()
    lr.fit(randomDf, features, label)
    val finalDf = lr.transform(randomDf)
    finalDf.show()
  }
}