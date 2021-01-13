package dbest

import java.io._
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import settings.Settings
import scala.collection.mutable.Map
import client._

object SensitivityAnalysisSampleSizeEffect {
  def main(args: Array[String]) {
  // Init settings and logger
    val appName = "Sensi. Analysis Sample Size"
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

  // Results directories
    val resultsDir = settings.resultsFolder
    val dirModelBased = resultsDir + "sensitive_analysis/sample_size/model_based/"
    val dirSampleBased = resultsDir + "sensitive_analysis/sample_size/sample_based/"
    val subdirErr = "relative_error/"
    val subdirTime = "response_time/"
    var errFileNameMB = ""
    var timeFileNameMB = ""
    var errFileNameSB = ""
    var timeFileNameSB = ""
    def errPathMB = dirModelBased + subdirErr + errFileNameMB
    def timePathMB = dirModelBased + subdirTime + timeFileNameMB
    def errPathSB = dirSampleBased + subdirErr  + errFileNameSB
    def timePathSB = dirSampleBased + subdirTime + timeFileNameSB

  // Experiment parameter
    val sampleSizes = List(0.001, 0.01, 0.1, 0.5, 1.0)
    val aggregationFunctions = List("count", "sum", "avg")

  // load queries
    val fileName = "experiments/sensitivity_analysis_sampleSize_queries.json"
    val jsonContent = scala.io.Source.fromFile(fileName).mkString
    val json: JsValue = Json.parse(jsonContent)

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
      tableName = "store_sales"
      client.loadTable(path, tableName, "csv")
    }
    val features = Array("ss_list_price")
    val label = "ss_wholesale_cost"

  // Experiment
    for (sampleSize <- sampleSizes) {
      logger.info(s"Sample size: $sampleSize")
    // Model-Based Results
      val errMapMB = Map[String, Double]()
      val timeMapMB = Map[String, Long]()
    // Sample-Based Results
      val errMapSB = Map[String, Double]()
      val timeMapSB = Map[String, Long]()

      aggregationFunctions.foreach { af =>
        errMapMB += af -> 0.0; timeMapMB += af -> 0L;
        errMapSB += af -> 0.0; timeMapSB += af -> 0L;
      }
      for (af <- aggregationFunctions) {
        val ranges = json(af.toUpperCase()).as[List[List[Double]]]
        val queriesAfNumber = ranges.length.toDouble
        for (l <- ranges) {
          val (a, b) = (l(0), l(1))
          val q = s"SELECT ${af.toUpperCase()}($label) FROM $tableName WHERE ${features(0)} BETWEEN $a AND $b"
          val resDf = client.query(q)
          val exactRes = if (af == "count") {
            resDf.take(1)(0)(0).asInstanceOf[Long].toDouble
          } else {
            resDf.take(1)(0)(0).asInstanceOf[Double]
          }

          val (approxResMB, timeMB) = client.queryWithModel(
            settings,
            af,
            features,
            label,
            a,
            b,
            sampleSize
          )
          val relErrMB = (exactRes - approxResMB) / exactRes
          errMapMB(af) += relErrMB / queriesAfNumber
          timeMapMB(af) += timeMB / queriesAfNumber.toLong

          val (approxResSB, timeSB) = client.queryWithSample(
            settings,
            af,
            features,
            label,
            a,
            b,
            sampleSize
          )
          val relErrSB = (exactRes - approxResSB) / exactRes
          errMapSB(af) += relErrSB / queriesAfNumber
          timeMapSB(af) += timeSB / queriesAfNumber.toLong    
        }
      }

      val finalErrMapMB = errMapMB.mapValues(v => if(v.isInfinite()) Double.MinValue else v)
      val finalTimeMapMB = timeMapMB.mapValues(v => if(v.isInfinite()) Double.MinValue else v)
      val finalErrMapSB = errMapSB.mapValues(v => if(v.isInfinite()) Double.MinValue else v)
      val finalTimeMapSB = timeMapSB.mapValues(v => if(v.isInfinite()) Double.MinValue else v)
      val errStringMB = Json.stringify(Json.toJson(finalErrMapMB))
      val timeStringMB = Json.stringify(Json.toJson(finalTimeMapMB))
      val errStringSB = Json.stringify(Json.toJson(finalErrMapSB))
      val timeStringSB = Json.stringify(Json.toJson(finalTimeMapSB))
      
    // Write Model-Based Results
      errFileNameMB = s"relative_error_$sampleSize.json"
      timeFileNameMB = s"response_time_$sampleSize.json"
      
      new PrintWriter(errPathMB) { write(errStringMB); close() }
      new PrintWriter(timePathMB) { write(timeStringMB); close() }
      
    // Write Sample-Based Results
      errFileNameSB = s"relative_error_$sampleSize.json"
      timeFileNameSB = s"relative_error_$sampleSize.json"
      
      new PrintWriter(errPathSB) { write(errStringSB); close() }
      new PrintWriter(timePathSB) { write(timeStringSB); close() }
      
      logger.info("errStringMB: " + errStringMB)
      logger.info("timeStringMB: " + timeStringMB)
      logger.info("errStringSB: " + errStringSB)
      logger.info("timeStringSB: " + timeStringSB)
    }
    client.close()
  }
}
