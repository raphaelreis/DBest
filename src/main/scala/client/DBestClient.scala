package client

import org.apache.log4j.Logger
import java.nio.file.{Paths, Files}
import java.io.FileNotFoundException
import scala.collection.mutable.Map
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}

import traits.Analyser
import settings.Settings
import engine.QueryEngine
import dbest.ml.ModelWrapper
import sampler.Sampler.uniformSampling

class DBestClient (settings: Settings, appName: String = "DBEst Client") extends Analyser {
  val logger = Logger.getLogger(this.getClass().getName())
  var trainingFrac = 1.0
  var df: DataFrame = _
  var trainingDF: DataFrame = _
  var dfSize: Long = _
  var dfMins = Map[String, Double]()
  var dfMaxs = Map[String, Double]()
  val spark: SparkSession = SparkSession.builder
    .appName(appName)
    .getOrCreate()
  val qe = new QueryEngine(spark)

  def setNewTrainingFrac(newTrainingFrac: Double) {
    if (newTrainingFrac != trainingFrac) {
      trainingFrac = newTrainingFrac
      updateTrainingDF()
      qe.setNewTrainingDF(trainingDF, trainingFrac)
    }
  }

  def updateTrainingDF() {
    if(trainingFrac != 1.0) {
      trainingDF = uniformSampling(df, trainingFrac).cache()
      trainingDF.count()
    } else {
      trainingDF = df.cache()
      trainingDF.count()
    }
  }

  def getOfflineStats(df: DataFrame) {
    dfSize = df.count()
    for (col <- df.columns) {
      val minMax = df.agg(F.min(col), F.max(col)).head.toSeq
      val (minimum: Double, maximum: Double) = (minMax(0).toString().toDouble, minMax(1).toString().toDouble)
      dfMins += col -> minimum
      dfMaxs += col -> maximum
    }
    qe.setStats(dfSize, dfMins, dfMaxs)
  }

  def loadHDFSTable(path: String, tableName: String, format: String = "csv") {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    logger.info("path: " + path)
    val fileExists = fs.exists(new Path(path))
    if (fileExists) {
      df = spark.read.format(format)
                .option("delimiter", "|")
                .load(path)
                .repartition((1 * settings.numberOfCores).toInt)
                .withColumn("ss_wholesale_cost", F.col("_c11").cast(DoubleType))
                .withColumn("ss_list_price", F.col("_c12").cast(DoubleType))
                .select("ss_list_price", "ss_wholesale_cost")
                .na.drop()
                // .cache()
      df.createOrReplaceTempView(tableName)
      getOfflineStats(df)
    } else {
      throw new FileNotFoundException("Table not found on HDFS")
    }
  }

  def loadTable(path: String, tableName: String, format: String = "csv") {
    if (Files.exists(Paths.get(path))) {
      df = spark.read.format(format)
            .option("delimiter", "|")
            .load("file:///" + path)
            .repartition((1 * settings.numberOfCores).toInt)
            .withColumn("ss_wholesale_cost", F.col("_c11").cast(DoubleType))
            .withColumn("ss_list_price", F.col("_c12").cast(DoubleType))
            .select("ss_list_price", "ss_wholesale_cost")
            .na.drop()
            //.cache()
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
      val (count, elipseTime) = qe.approxCountBySampling(features, label, a, b)
      (count, elipseTime)
    }
    case "sum" => {
      //Aggregation evaluation
      val (sum, elipseTime) = qe.approxSumBySampling(features, label, a, b)
      (sum, elipseTime)
    }
    case "avg" => {
      //Aggregation evaluation
      val (avg, elipseTime) = qe.approxAvgBySampling(features, label, a, b)
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
      val (count, elipseTime) = qe.approxCount(mw, features, label, a, b)
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
      val (sum, elipseTime) = qe.approxSum(mw, features, label, a, b)
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
      val (avg, elipseTime) = qe.approxAvg(mw, x, y, a, b)
      (avg, elipseTime)
    }
    case _ =>
      throw new NotImplementedError(
        "Please choose between count, sum and avg aggregation function"
      )
  }

  def close() = {
    spark.stop()
  }
}
