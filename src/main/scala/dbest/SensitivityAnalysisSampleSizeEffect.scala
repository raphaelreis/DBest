package dbest

import java.io._
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import settings.Settings
import scala.collection.mutable.Map

object SensitivityAnalysisSampleSizeEffect {
  def main(args: Array[String]) {
    // Results directories
    val dir = "results/sensitive_analysis/sample_size/"
    val subdirErr = "relative_error/"
    val subdirTime = "response_time/"

    // Init settings and logger
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

    // Experiment parameter
    val sampleSizes = List(0.001, 0.01, 0.1, 0.5, 1.0)
    val aggregationFunctions = List("count", "sum", "avg")

    // load queries
    val fileName = "experiments/sensitivity_analysis_sampleSize_queries.json"
    val jsonContent = scala.io.Source.fromFile(fileName).mkString
    val json: JsValue = Json.parse(jsonContent)

    val client: DBestClient.DBestClient = new DBestClient.DBestClient
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

    for (sampleSize <- sampleSizes) {
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

      val errString = Json.stringify(Json.toJson(resMap))
      val timeString = Json.stringify(Json.toJson(timeMap))
      val errWriteName = dir + subdirErr + s"relative_error_$sampleSize.json"
      val timeWriteName = dir + subdirTime + s"response_time_$sampleSize.json"
      new PrintWriter(errWriteName) { write(errString); close() }
      new PrintWriter(timeWriteName) { write(timeString); close() }
    }

  }
}
