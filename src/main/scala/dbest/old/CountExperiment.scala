package dbest

import java.io._
import client.DBestClient
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import com.typesafe.config._
import settings.Settings



object CountExperiment {
  def main(args: Array[String]) = {

    // Init
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

    // Parameters
    val distribution = "uniform"
    var (a, b) = (45.0, 65.0)
    val maxDataSample = 0.125
    val agg = "count"
    
    
    
    val client: DBestClient = new DBestClient(settings)
    var path = ""
    var tableName = ""
    if (settings.hdfsAvailable) {
      path = s"data2/df10m_$distribution.parquet"
      tableName = s"dataframe10m_$distribution"
      client.loadHDFSTable(path, tableName)
    } else {
      path = "data/df.parquet"
      tableName = "df"
      client.loadTable(path, tableName)
    }
    logger.info(s"Table: $path")
    val features = Array(distribution)
    val label = "label"

    // Exact counter
    val q1 = s"SELECT ${agg.toUpperCase()}(*) FROM $tableName WHERE ${features(0)} BETWEEN $a AND $b"
    val res = client.query(q1)
    val t0 = System.nanoTime()
    val exactCount = res.take(1)(0)(0).asInstanceOf[Long].toDouble
    logger.info(s"exactCount: $exactCount")
    val t1 = System.nanoTime()
    val elapsedTimeExact = (t1-t0).toLong * 1E-9

    // AQL: Model trained with a eighth of the data
    logger.info("Experiment1 !")
    val (count1, elapsedTime1) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample)

    // AQL: Model trained with a sixteenth of the data
    logger.info("Experiment2 !")
    val (count2, elapsedTime2) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 2)

    // AQL: Model trained with ...
    logger.info("Experiment3 !")
    val (count3, elapsedTime3) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 4)

    // AQL: Model trained with ...
    logger.info("Experiment4 !")
    val (count4, elapsedTime4) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 8)

    // AQL: Model trained with ...
    logger.info("Experiment5 !")
    val (count5, elapsedTime5) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 16)

    // AQL: Model trained with ...
    logger.info("Experiment6 !")
    val (count6, elapsedTime6) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 32)
  
    // Format and write data
    val aggMode = List("exact", s"model $maxDataSample%", s"model ${maxDataSample/2}%",
        s"model ${maxDataSample/4}%", s"model ${maxDataSample/8}%",
        s"model ${maxDataSample/16}%", s"model ${maxDataSample/32}%")
    val cv = List(exactCount, count1, count2, count3, count4, count5, count6)
    val timing = List(elapsedTimeExact, elapsedTime1 * 1E-9, elapsedTime2 * 1E-9,
      elapsedTime3 * 1E-9, elapsedTime4 * 1E-9, elapsedTime5 * 1E-9, elapsedTime6 * 1E-9)
    def relativeErrorFun = (realError: Double, approxError: Double) => (approxError - realError) / realError * 100.0
    val relativeError = 0.0 +: cv.tail.map(relativeErrorFun(exactCount, _))

    val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    val outputFile = settings.resultsFolder + s"$tableName" + "_" + s"${a.toInt}" + "_"+ s"${b.toInt}" + "_" + s"$agg" + "_" + timestamp + ".csv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)))
    val header = "Agg mode,Count value,Time, RelError(%)\n"
    writer.write(header)
    for ((((c1, c2), c3), c4) <- aggMode zip cv zip timing zip relativeError) {
      val line: String = c1.toString + f",$c2%.2f,$c3%3.2f,$c4%.2f\n"
      writer.write(line)
    }
    writer.close()
    client.close()
  }
}