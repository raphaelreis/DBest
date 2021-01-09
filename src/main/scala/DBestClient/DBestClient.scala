package DBestClient

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => F}
import org.apache.spark.mllib.stat.KernelDensity
import Ml._
import QueryEngine._
import DataLoader._
import org.apache.spark.ml.regression.LinearRegressionModel
import DataGenerator.DataGenerator._
import javassist.NotFoundException
import java.io.FileNotFoundException
import Sampler.Sampler._
import settings.Settings
import scala.collection.mutable.Map
import traits.Analyser

class DBestClient extends Analyser {
  val logger = Logger.getLogger(this.getClass().getName())
  var df: DataFrame = _
  var dfSize: Long = _
  var dfMins = Map[String, Double]()
  var dfMaxs = Map[String, Double]()

  val spark: SparkSession = SparkSession.builder
    .master("local")
    .appName("DBest client")
    .getOrCreate()

  def close() = {
    spark.stop()
  }

  def getOfflineStats(df: DataFrame) {
    dfSize = df.count()
    for (col <- df.columns) {
      val minMax = df.agg(F.min(col), F.max(col)).head.toSeq
      val (minimum: Double, maximum: Double) =
        (minMax(0).toString().toDouble, minMax(1).toString().toDouble)
      dfMins += col -> minimum
      dfMaxs += col -> maximum
    }
  }

  def loadHDFSTable(path: String, tableName: String) {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(path))
    if (fileExists) {
      df = spark.read.parquet(path).cache()
      df.createOrReplaceTempView(tableName)
      getOfflineStats(df)
    } else {
      throw new FileNotFoundException("Table not found on HDFS")
    }
  }

  def loadTable(path: String, tableName: String, format: String = "parquet") {
    val dload = new DataLoader

    if (Files.exists(Paths.get(path))) {
      df = dload.loadTable(spark, path, tableName, format)
      getOfflineStats(df)
    } else {
      throw new FileNotFoundException("Table does not exist on the given path")
    }
  }

  def query(q: String) = {
    val res = spark.sql(q)
    spark.time(res.show())
    res
  }

  def queryWithModel(
      settings: Settings,
      aggFun: String,
      features: Array[String],
      label: String,
      a: Double,
      b: Double,
      sampleSize: Int
  ): (Double, Long) = {
    val trainingFrac = sampleSize.toDouble / dfSize.toDouble
    queryWithModel(settings, aggFun, features, label, a, b, trainingFrac)
  }

  def queryWithModel(
      settings: Settings,
      aggFun: String,
      features: Array[String],
      label: String,
      a: Double,
      b: Double,
      trainingFrac: Double = 1.0
  ) = aggFun match {
    case "count" => {
      val x = features

      //Model fitting
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("count", a, b, df, x, label, trainingFrac)

      // Aggregation evaluation
      val qe = new QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (count, elipseTime) = qe.approxCount(df, mw, x, label, a, b, 0.01)
      (count, elipseTime)
    }
    case "sum" => {
      val x = features
      val y = label

      // Dataframe processing
      val dp = new DataProcessor.DataProcessor(df, x, y)
      val processedDf = dp.processForRegression().getPreprocessedDF()

      // Model fitting
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("sum", a, b, processedDf, x, y, trainingFrac)

      //Aggregation evaluation
      val qe = new QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (sum, elipseTime) = qe.approxSum(df, mw, x, y, a, b, 0.01)
      (sum, elipseTime)
    }
    case "avg" => {
      val x = features
      val y = label

      // Dataframe processing
      val dp = new DataProcessor.DataProcessor(df, x, y)
      val processedDf = dp.processForRegression().getPreprocessedDF()

      // Model fitting
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("avg", a, b, processedDf, x, y, trainingFrac)

      //Aggregation evaluation
      val qe = new QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (avg, elipseTime) = qe.approxAvg(df, mw, x, y, a, b, 0.01)
      (avg, elipseTime)
    }
    case _ =>
      throw new NotImplementedError(
        "Please choose between count, sum and avg aggregation function"
      )
  }
}

// def simpleQuery1(A: Double, B: Double) {
//     /** Run simple count query with filtering */
//     val q1 = s"SELECT COUNT(*) FROM store_sales WHERE _c12 BETWEEN $A AND $B"
//     val res1 = spark.sqlContext.sql(q1)
//     spark.time(res1.show())
// }

// def simpleQuery1WithModel(A: Double, B: Double) {
//     /**
//       * Same as simpleQuery1 but with AQP
//       */

//     val x = Array("_c12")
//     val y = "_c20"

//     // val models = new ModelWrapper()
//     // models.fitOrLoad(df, x, y)

//     val d = new SparkKernelDensity(3.0)
//     val kde = d.fit(df, x)
//     // val lr = new LinearRegressor
//     // val lrm = lr.fit(df, x, y)
//     val qe = new QueryEngine(spark, df.count().toInt)

//     val (count, elipseTime) = qe.approxCount(kde, A, B, 0.01)

//     println(s"Count value with model: $count")
//     println(s"Time to compute count: $elipseTime")
// }

// def simpleQuery2() {
//     val q2 = "SELECT AVG(_c20) FROM store_sales WHERE _c13 BETWEEN 5 AND 70"
//     val res2 = spark.sqlContext.sql(q2)
//     spark.time(res2.show())
// }

// def simpleQuery2WithModel() {

//     val x = Array("_c13")
//     val y = "_c20"

//     val d = new SparkKernelDensity(3.0)
//     val kde = d.fit(df, x)
//     val lr = new LinearRegressor
//     val lrm = lr.fit(df, x, y)

//     val qe = new QueryEngine(spark, df.count().toInt)
//     val (avg, elipseTime) = qe.approxAvg(df, kde, lrm, x, 5, 70, 0.01)

//     println(s"AVG value with model: $avg")
//     println(s"Time to compute count: $elipseTime")
// }

// def simpleQuery3() {
//     val q3 = "SELECT SUM(_c20) FROM store_sales WHERE _c13 BETWEEN 5 AND 70"
//     val res3 = spark.sqlContext.sql(q3)
//     spark.time(res3.show())
// }

// def simpleQuery3WithModel() {

//     val x = Array("_c13")
//     val y = "_c20"

//     val d = new SparkKernelDensity(3.0)
//     val kde = d.fit(df, x)
//     val lr = new LinearRegressor
//     val lrm = lr.fit(df, x, y)

//     val qe = new QueryEngine(spark, df.count().toInt)
//     val (sum, elipseTime) = qe.approxSum(df, kde, lrm, x, 5, 70, 0.01)

//     println(s"SUM value with model: $sum")
//     println(s"Time to compute count: $elipseTime")

// }
