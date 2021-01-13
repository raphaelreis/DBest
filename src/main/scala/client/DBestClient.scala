package client

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{functions => F}
import org.apache.spark.mllib.stat.KernelDensity
import dbest.ml.ModelWrapper
import engine._
import DataLoader._
import org.apache.spark.ml.regression.LinearRegressionModel
import javassist.NotFoundException
import java.io.FileNotFoundException
import Sampler.Sampler._
import settings.Settings
import scala.collection.mutable.Map
import traits.Analyser
import org.apache.spark.sql.types.DoubleType

class DBestClient (settings: Settings, appName: String = "DBEst Client") extends Analyser {
  val logger = Logger.getLogger(this.getClass().getName())
  var df: DataFrame = _
  var dfSize: Long = _
  var dfMins = Map[String, Double]()
  var dfMaxs = Map[String, Double]()

  val spark: SparkSession = SparkSession.builder
    .appName(appName)
    .getOrCreate()

  def close() = {
    spark.stop()
  }

  def getOfflineStats(df: DataFrame) {
    dfSize = df.count()
    for (col <- df.columns) {
      val minMax = df.agg(F.min(col), F.max(col)).head.toSeq
      val (minimum: Double, maximum: Double) = (minMax(0).toString().toDouble, minMax(1).toString().toDouble)
      dfMins += col -> minimum
      dfMaxs += col -> maximum
    }
  }

  def loadHDFSTable(path: String, tableName: String) {

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val fileExists = fs.exists(new Path(path))
    if (fileExists) {
      df = spark.read.parquet(path)
                .repartition((4 * settings.numberOfCores).toInt)
                .cache()
      df.createOrReplaceTempView(tableName)
      getOfflineStats(df)
    } else {
      throw new FileNotFoundException("Table not found on HDFS")
    }
  }

  def loadTable(path: String, tableName: String, format: String = "parquet") {
    val dload = new DataLoader

    if (Files.exists(Paths.get(path))) {
      val tmpDf = dload.loadTable(spark, path, tableName, format)
                .repartition((4 * settings.numberOfCores).toInt)
      // Temporary implementation specific to store_sales.dat
      df = tmpDf.withColumn("ss_wholesale_cost", F.col("_c11").cast(DoubleType))
              .withColumn("ss_list_price", F.col("_c12").cast(DoubleType))
              .select("ss_list_price", "ss_wholesale_cost")
              .cache()
      ///
      df.createOrReplaceTempView(tableName)
      getOfflineStats(df)
    } else {
      throw new FileNotFoundException("Table does not exist on the path: " + path)
    }
  }

  def query(q: String) = {
    val res = spark.sql(q)
    spark.time(res.show())
    res
  }

  def queryWithSample(settings: Settings,
      aggFun: String,
      features: Array[String],
      label: String,
      a: Double,
      b: Double,
      trainingFrac: Double = 1.0
  ) = aggFun match {
    case "count" => {
      // Aggregation evaluation
      val qe = new engine.QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (count, elipseTime) = qe.approxCountBySampling(df, features, label, a, b, trainingFrac)
      (count, elipseTime)
    }
    case "sum" => {
      //Aggregation evaluation
      val qe = new engine.QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (sum, elipseTime) = qe.approxSumBySampling(df, features, label, a, b, trainingFrac)
      (sum, elipseTime)
    }
    case "avg" => {
      //Aggregation evaluation
      val qe = new engine.QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (avg, elipseTime) = qe.approxAvgBySampling(df, features, label, a, b, trainingFrac)
      (avg, elipseTime)
    }
    case _ =>
      throw new NotImplementedError(
        "Please choose between count, sum and avg aggregation function"
      )
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
      //Model fitting
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("count", df, features, label, trainingFrac)

      // Aggregation evaluation
      val qe = new engine.QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (count, elipseTime) = qe.approxCount(df, mw, features, label, a, b, 0.01)
      (count, elipseTime)
    }
    case "sum" => {
      // Dataframe processing
      val dp = new dbest.dataprocessor.DataProcessor(df, features, label)
      val processedDf = dp.processForRegression().getPreprocessedDF()

      // Model fitting
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("sum", processedDf, features, label, trainingFrac)

      //Aggregation evaluation
      val qe = new engine.QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (sum, elipseTime) = qe.approxSum(df, mw, features, label, a, b, 0.01)
      (sum, elipseTime)
    }
    case "avg" => {
      val x = features
      val y = label

      // Dataframe processing
      val dp = new dbest.dataprocessor.DataProcessor(df, x, y)
      val processedDf = dp.processForRegression().getPreprocessedDF()

      // Model fitting
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("avg", processedDf, x, y, trainingFrac)

      //Aggregation evaluation
      val qe = new engine.QueryEngine(spark, dfSize, dfMins, dfMaxs)
      val (avg, elipseTime) = qe.approxAvg(df, mw, x, y, a, b, 0.01)
      (avg, elipseTime)
    }
    case _ =>
      throw new NotImplementedError(
        "Please choose between count, sum and avg aggregation function"
      )
  }
}
