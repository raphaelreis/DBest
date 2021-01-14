package dbest

import better.files._
import org.apache.log4j.Logger
import play.api.libs.json.Json
import com.typesafe.config.ConfigFactory
import java.io.{File => JFile, PrintWriter}
import com.typesafe.config.ConfigValueFactory
import scala.collection.mutable.Map
import dbest.ml.ModelWrapper

import settings.Settings
import client.DBestClient
import dbest.dataprocessor.DataProcessor


object TrainModels {
  def main(args: Array[String]) {
    // Init settings and logger
    val appName = "Models Training"
    val logger = Logger.getLogger(this.getClass().getName())
    val confPath = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new JFile(confPath)).resolve()
    val settings = new Settings(conf)

    // Training sample size
    val sampleSize = List(0.001, 0.01, 0.1, 0.5, 1.0)
    // val sampleSize = List(0.5, 1.0)

    // Init training
    val client = new DBestClient(settings, appName)
    // var path = if(args.length == 1) System.getProperty("user.dir") + "/" + args(0) else ""
    var path = if(args.length == 1) "hdfs:///" + args(0) else ""
    var tableName = ""
    if (settings.hdfsAvailable) {
      path = if (path.isEmpty()) "hdfs:///data/store_sales.dat" else path
      tableName = s"store_sales_sf10"
      client.loadHDFSTable(path, tableName)
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
    val dp = new DataProcessor(df, features, label)
    val processedDf = dp.processForRegression().getPreprocessedDF()

    // Training
    val t0 = System.nanoTime()
    for (trainingFrac <- sampleSize.sorted) {
      logger.info(s"Training at $trainingFrac of the data")
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("sum", processedDf, features, label, trainingFrac)
    }
    val t1 = System.nanoTime()

    logger.info("Training time: " + ((t1-t0)*1E-9).toString())
    client.close()
  }
}