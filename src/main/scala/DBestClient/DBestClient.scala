package DBestClient

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.stat.KernelDensity
import Ml._
import QueryEngine._
import org.apache.spark.ml.regression.LinearRegressionModel


class DBestClient(dataset: String) {

    var df: DataFrame = _

    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("DBest client")
      .getOrCreate()
    
    def loadDataset(dataset: String): Unit = {
        df = spark.read.format("csv")
            .option("header", false)
            .option("delimiter", "|")
            .option("inferSchema", "true")
            .option("mode", "DROPMALFORMED")
            .load(dataset).drop("_c23")
        df.createOrReplaceTempView("store_sales")
        df = df.na.drop().cache()
    }

    if (Files.exists(Paths.get(dataset))) {
        loadDataset(dataset)
    } else {
        throw new Exception("Dataset does not exist.")
    }

    //sampling
    // val fraction = 0.2
    // val sampler = new Sampler(df)
    // val sampled_df = sampler.uniformSampling(fraction)
    // logger.info("sampled_df.count(): " + sampled_df.count())



    def simpleQuery1() {
        /** Run simple count query with filtering */
        val q1 = "SELECT COUNT(*) FROM store_sales WHERE _c12 BETWEEN 50 AND 100"
        val res1 = spark.sqlContext.sql(q1)
        spark.time(res1.show())
    }

    def simpleQuery1WithModel() {
        /**
          * Same as simpleQuery1 but with AQP
          */
        val d = new SparkKernelDensity(3.0)
        val kde = d.fit(df, Array("_c12"))
        val lr = new LinearRegressor
        val qe = new QueryEngine(spark, kde, lr.model, df.count().toInt)

        val (count, elipseTime) = qe.approxCount(50, 100, 0.01)

        println(s"Count value with model: $count")
        println(s"Time to compute count: $elipseTime")

    }

    def simpleQuery2() {
        val q2 = "SELECT AVG(_c20) FROM store_sales WHERE _c13 BETWEEN 5 AND 70"
        val res2 = spark.sqlContext.sql(q2)
        spark.time(res2.show())
    }

    def simpleQuery2WithModel() {

        val x = Array("_c13")
        val y = "_c20"

        val d = new SparkKernelDensity(3.0)
        val kde = d.fit(df, x)
        val lr = new LinearRegressor
        val lrm = lr.fit(df, x, y)

        val qe = new QueryEngine(spark, kde, lrm, df.count().toInt)
        val (avg, elipseTime) = qe.approxAvg(df, x, 5, 70, 0.01)

        println(s"AVG value with model: $avg")
        println(s"Time to compute count: $elipseTime")

    }

    def simpleQuery3() {
        val q3 = "SELECT SUM(_c20) FROM store_sales WHERE _c13 BETWEEN 5 AND 70"
        val res3 = spark.sqlContext.sql(q3)
        spark.time(res3.show())
    }

    def simpleQuery3WithModel() {

        val x = Array("_c13")
        val y = "_c20"

        val d = new SparkKernelDensity(3.0)
        val kde = d.fit(df, x)
        val lr = new LinearRegressor
        val lrm = lr.fit(df, x, y)

        val qe = new QueryEngine(spark, kde, lrm, df.count().toInt)
        val (sum, elipseTime) = qe.approxSum(df, x, 5, 70, 0.01)

        println(s"AVG value with model: $avg")
        println(s"Time to compute count: $elipseTime")

    }
}