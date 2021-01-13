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

    // Init training
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

    // Training
    val t0 = System.nanoTime()
    for (trainingFrac <- sampleSize) {
      val mw = new ModelWrapper(settings, dfSize, dfMins, dfMaxs)
      mw.fitOrLoad("sum", processedDf, features, label, trainingFrac)
    }
    val t1 = System.nanoTime()

    logger.info("Training time: " + ((t1-t0)*1E-9).toString())
    client.close()

  }
}