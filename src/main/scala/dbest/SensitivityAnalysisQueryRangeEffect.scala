package dbest

import org.apache.log4j.Logger
import play.api.libs.json.Json
import java.io.{File, PrintWriter}
import scala.collection.mutable.Map
import com.typesafe.config.ConfigFactory

import settings.Settings
import client.DBestClient


object SensitivityAnalysisQueryRangeEffect {
  def main(args: Array[String]) {
  // Init settings and logger
    val appName = "Sensi. Analysis Query Range Effect"
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

  // Results directories
    val dir = settings.resultsFolder + "sensitive_analysis/query_range/"
    val subdirErr = "relative_error/"
    val subdirTime = "response_time/"
    var errFileName = ""
    var timeFileName = ""
    def errPath = dir + subdirErr + errFileName
    def timePath = dir + subdirTime + timeFileName

  // Experiment parameters
    val ranges = List(0.001, 0.01, 0.1, 0.5, 1.0)
    val sampleSize = 0.1
    val aggregationFunctions = List("count", "sum", "avg")

  // Experiment initialization
    val client: DBestClient = new DBestClient(settings, appName)
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

    for (range <- ranges) {
    // Experiment
    //// load queries
      val fileName =
        s"experiments/sensitivity_analysis_queryRange${range}_queries.json"
      val jsonContent = scala.io.Source.fromFile(fileName).mkString
      val json = Json.parse(jsonContent)

      val errMap = Map[String, Double]()
      val timeMap = Map[String, Long]()
      aggregationFunctions.foreach { af =>
        errMap += af -> 0.0; timeMap += af -> 0L
      }

      for (af <- aggregationFunctions) {
        val ranges = json(af.toUpperCase()).as[List[List[Double]]]
        val queriesAfNumber = ranges.length.toDouble
        for (l <- ranges) {
          val (a, b) = (l(0), l(1))
          val q =
            s"SELECT ${af.toUpperCase()}($label) FROM $tableName WHERE ${features(0)} BETWEEN $a AND $b"
          val resDf = client.query(q)
          val exactRes = if (af == "count") {
            resDf.take(1)(0)(0).asInstanceOf[Long]
          } else {
            resDf.take(1)(0)(0).asInstanceOf[Double]
          }
          val (approxRes, time) = client.queryWithModel(
            settings,
            af,
            features,
            label,
            a,
            b,
            sampleSize
          )
          val relErr = (exactRes - approxRes) / exactRes
          errMap(af) += relErr / queriesAfNumber
          timeMap(af) += time / queriesAfNumber.toLong
        }
      }

      logger.info("errMap: " + errMap.mkString(","))
      logger.info("timeMap: " + timeMap.mkString(","))
      val finalErrMap = errMap.mapValues(v => if(v.isInfinity) Double.MinValue else v)
      val finalTimeMap = timeMap.mapValues(v => if(v.isInfinite) Double.MinValue else v)
      val errString = Json.stringify(Json.toJson(finalErrMap))
      val timeString = Json.stringify(Json.toJson(finalTimeMap))
      
      errFileName = s"relative_error_$range.json"
      timeFileName = s"response_time_$range.json"
      
      new PrintWriter(errPath) { write(errString); close() }
      new PrintWriter(timePath) { write(timeString); close() }
    
    }
    client.close()
  }
}
