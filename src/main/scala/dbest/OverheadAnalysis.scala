package dbest

import java.io.{File => JFile, PrintWriter}
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import settings.Settings
import scala.collection.mutable.Map
import dbest.ml.ModelWrapper
import better.files._
import client._

object OverheadAnalysis {
  def main(args: Array[String]): Unit = {
  // Init settings and logger
    val appName = "Overhead Analysis"
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val newDensityPathConfig = ConfigFactory.parseString("app.densitiesPath=${basedir}models/overhead_models/densities/")
    val newRegressionPathConfig = ConfigFactory.parseString("app.regressionPath=${basedir}models/overhead_models/regressions/")
    val oldConf = ConfigFactory.parseFile(new JFile(confFileName))
    val conf = newDensityPathConfig.withFallback(newRegressionPathConfig).withFallback(oldConf).resolve()             
    val settings = new Settings(conf)

  // Results directories
    val dir = settings.resultsFolder + "overhead_analysis/"
    val subdirSpace = dir + "space_overhead/"
    val subdirTime = dir + "training_time/"
    val timeFileName = "time_vs_sample_size.json"
    val spaceFileName = "space_overhead_vs_sample_size.json"
    val timePath = subdirTime + timeFileName
    val spacePath = subdirSpace + spaceFileName

  // Experiment parameters
    val sampleSize = List(0.001, 0.01, 0.1, 0.5, 1.0)
    
  // Experiment initialization
    val client = new DBestClient(settings, appName)
    var path = if(args.length == 1) System.getProperty("user.dir") + "/" + args(0) else ""
    var tableName = ""
    if (settings.hdfsAvailable) {
      // path = s"data/${agg}_df_${distribution}_label_10m.parquet"
      // tableName = s"${agg}_df_${distribution}_label_10m"
      // client.loadHDFSTable(path, tableName)
      println("hello world")
    } else {
      path = if (path.isEmpty) System.getProperty("user.dir") + "/data/store_sales_sample.dat" else path
      tableName = "store_sales_sample"
      client.loadTable(path, tableName, "csv")
    }
    val features = Array("ss_list_price")
    val label = "ss_wholesale_cost"
    val df = client.df
    var dfSize = client.dfSize
    var dfMins = client.dfMins
    var dfMaxs = client.dfMaxs

  // Dataframe processing
    val dp = new dbest.dataprocessor.DataProcessor(df, features, label)
    val processedDf = dp.processForRegression().getPreprocessedDF()

  // Experiment
    val spaceMap = Map[String, Long]()
    val timeMap = Map[String, Long]()
    sampleSize.foreach { ss =>
      spaceMap += ss.toString -> 0L; timeMap += ss.toString -> 0L
    }
    val densitiesDir = (settings.dpath).toFile
    val regressionsDir = (settings.rpath).toFile

    for (trainingFrac <- sampleSize) {
      densitiesDir.clear()
      regressionsDir.clear()
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      val t0 = System.nanoTime()
      mw.fitOrLoad("sum", processedDf, features, label, trainingFrac)
      val t1 = System.nanoTime()
      densitiesDir.size()
      timeMap += trainingFrac.toString() -> (t1 - t0)
      spaceMap += trainingFrac.toString() -> (densitiesDir.size() + regressionsDir.size())
    }


    val finalTimeMap = timeMap.mapValues(v => if(v.isInfinity) Double.MinValue else v)
    val finalSpaceMap = spaceMap.mapValues(v => if(v.isInfinity) Double.MinValue else v)
    val timeString = Json.stringify(Json.toJson(finalTimeMap))
    val spaceString = Json.stringify(Json.toJson(finalSpaceMap))


    new PrintWriter(timePath) { write(timeString); close() }
    new PrintWriter(spacePath) { write(spaceString); close() } 
    
    logger.info("timeString: " + timeString)
    logger.info("spaceString: " + spaceString)

    client.close   
  }
}