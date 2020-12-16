package DBestClient

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.stat.KernelDensity
import Ml._
import QueryEngine._
import DataLoader._
import org.apache.spark.ml.regression.LinearRegressionModel
import DataGenerator.DataGenerator._
import javassist.NotFoundException
import java.io.FileNotFoundException
import Sampler.Sampler._


class DBestClient {
    val logger = Logger.getLogger(this.getClass().getName())
    var df: DataFrame = _
    var dfSize: Int = _

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("DBest client")
        .getOrCreate()

    def close() = {
        spark.stop()
    }

    def loadHDFSTable(path: String, tableName: String) {

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val fileExists = fs.exists(new Path(path))
        if (fileExists) {
            df = spark.read.parquet(path)
            df.createOrReplaceTempView(tableName)
            dfSize = df.count().toInt
        }
        else {
            throw new FileNotFoundException("Table not found on HDFS")
        }
    }

    def loadTable(path: String, tableName: String) {
        val dload = new DataLoader

        if (Files.exists(Paths.get(path))) {
            df = dload.loadTable(spark, path, tableName)
            dfSize = df.count().toInt
        } else {
            throw new FileNotFoundException("Table does not exist on the given path")
        }
    }

    def query(q: String) = {
        val res = spark.sql(q)
        spark.time(res.show())
        res
    }

    def queryWithModel(aggFun: String, features: Array[String], A: Double,
                        B: Double, trainingFrac: Double = 1.0) = aggFun match {
        case "count" => {
            val x = features
            var trainingDF = df

            // Get training fraction
            if (trainingFrac != 1.0) trainingDF = uniformSampling(df, trainingFrac)

            // Model fitting
            val d = new SparkKernelDensity(3.0)
            val kde = d.fit(trainingDF, x)

            // Aggregation evaluation
            val qe = new QueryEngine(spark, dfSize)
            val (count, elipseTime) = qe.approxCount(kde, A, B, 0.01)
            (count, elipseTime)
        }
        case "avg" => throw new NotImplementedError
        case "sum" => throw new NotImplementedError
        case _ => throw new NotImplementedError("Please choose between count, sum and avg aggregation function")
    }

    def simpleQuery1(A: Double, B: Double) {
        /** Run simple count query with filtering */
        val q1 = s"SELECT COUNT(*) FROM store_sales WHERE _c12 BETWEEN $A AND $B"
        val res1 = spark.sqlContext.sql(q1)
        spark.time(res1.show())
    }

    def simpleQuery1WithModel(A: Double, B: Double) {
        /**
          * Same as simpleQuery1 but with AQP
          */

        val x = Array("_c12")
        val y = "_c20"

        // val models = new ModelWrapper()
        // models.fitOrLoad(df, x, y)

        val d = new SparkKernelDensity(3.0)
        val kde = d.fit(df, x)
        // val lr = new LinearRegressor
        // val lrm = lr.fit(df, x, y)
        val qe = new QueryEngine(spark, df.count().toInt)

        val (count, elipseTime) = qe.approxCount(kde, A, B, 0.01)

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

        val qe = new QueryEngine(spark, df.count().toInt)
        val (avg, elipseTime) = qe.approxAvg(df, kde, lrm, x, 5, 70, 0.01)

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

        val qe = new QueryEngine(spark, df.count().toInt)
        val (sum, elipseTime) = qe.approxSum(df, kde, lrm, x, 5, 70, 0.01)

        println(s"SUM value with model: $sum")
        println(s"Time to compute count: $elipseTime")

    }
}