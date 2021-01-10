package dbest

import java.io.{File => JFile, PrintWriter}
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import settings.Settings
import scala.collection.mutable.Map
import ml.ModelWrapper
import better.files._
import client._

object OverheadAnalysis {
  def main(args: Array[String]): Unit = {
    // Results directories
    val dir = "results/overhead_analysis/"
    val subdirSpace = "space_overhead/"
    val subdirTime = "training_time/"

    // Init settings and logger
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val newDensityPathConfig = ConfigFactory.parseString("app.densitiesPath=${basedir}models/overhead_models/densities/")
    val newRegressionPathConfig = ConfigFactory.parseString("app.regressionPath=${basedir}models/overhead_models/regressions/")
    val oldConf = ConfigFactory.parseFile(new JFile(confFileName))
    val conf = newDensityPathConfig.withFallback(newRegressionPathConfig).withFallback(oldConf).resolve()             
    val settings = new Settings(conf)

    // Experiment parameters
    val sampleSize = List(0.001, 0.01, 0.1, 0.5, 1.0)
    
    // Experiment initialization
    val client = new DBestClient(settings)
    var path = ""
    var tableName = ""
    if (settings.hdfsAvailable) {
      // path = s"data/${agg}_df_${distribution}_label_10m.parquet"
      // tableName = s"${agg}_df_${distribution}_label_10m"
      // client.loadHDFSTable(path, tableName)
      println("hello world")
    } else {
      path = "data/store_sales_sample_processed.parquet"
      tableName = "store_sales_sample"
      client.loadTable(path, tableName)
    }
    val features = Array("ss_list_price")
    val label = "ss_wholesale_cost"
    val df = client.df
    var dfSize = client.dfSize
    var dfMins = client.dfMins
    var dfMaxs = client.dfMaxs

    // Dataframe processing
    val dp = new DataProcessor.DataProcessor(df, features, label)
    val processedDf = dp.processForRegression().getPreprocessedDF()

    // Experiment
    val spaceMap = Map[String, Long]()
    val timeMap = Map[String, Long]()
    sampleSize.foreach { ss =>
      spaceMap += ss.toString -> 0L; timeMap += ss.toString -> 0L
    }
    val densitiesDir = settings.dpath.toFile
    val regressionsDir = settings.rpath.toFile

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

    val timeString = Json.stringify(Json.toJson(timeMap))
    val spaceString = Json.stringify(Json.toJson(spaceMap))
    val timeWriteName = dir + subdirTime + s"time_vs_sample_size.json"
    val spaceWriteName = dir + subdirSpace + s"space_overhead_vs_sample_size.json"
    new PrintWriter(timeWriteName) { write(timeString); close() }
    new PrintWriter(spaceWriteName) { write(spaceString); close() }    
  }
}