package dbest

import java.io._
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import settings.Settings
import scala.collection.mutable.Map
import client._

object SensitivityAnalysisQueryRangeEffect {
  def main(args: Array[String]) {
    // Results directories
    val dir = "results/sensitive_analysis/query_range/"
    val subdirErr = "relative_error/"
    val subdirTime = "response_time/"

    // Init settings and logger
    val appName = "Sensi. Analysis Query Range Effect"
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

    // Experiment parameters
    val ranges = List(0.001, 0.01, 0.1, 0.5, 1.0)
    val sampleSize = 0.1
    val aggregationFunctions = List("count", "sum", "avg")

    // Experiment initialization
    val client: DBestClient = new DBestClient(settings, appName)
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

    for (range <- ranges) {
      // Experiment
      //// load queries
      val fileName =
        s"experiments/sensitivity_analysis_queryRange${range}_queries.json"
      val jsonContent = scala.io.Source.fromFile(fileName).mkString
      val json: JsValue = Json.parse(jsonContent)

      val resMap = Map[String, Double]()
      val timeMap = Map[String, Long]()
      aggregationFunctions.foreach { af =>
        resMap += af -> 0.0; timeMap += af -> 0L
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
          resMap(af) += relErr / queriesAfNumber
          timeMap(af) += time / queriesAfNumber.toLong
        }
      }

      println(resMap)
      val errString = Json.stringify(Json.toJson(resMap))
      val timeString = Json.stringify(Json.toJson(timeMap))
      val errWriteName = dir + subdirErr + s"relative_error_$range.json"
      val timeWriteName = dir + subdirTime + s"response_time_$range.json"
      new PrintWriter(errWriteName) { write(errString); close() }
      new PrintWriter(timeWriteName) { write(timeString); close() }
    }
  }
}
