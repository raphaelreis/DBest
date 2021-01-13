package dbest

import java.io._
import client.DBestClient
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}
import com.typesafe.config._
import settings.Settings

object AvgExperiment {
  def main(args: Array[String]) {
    // Init
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

    // Parameters
    val distribution = "uniform"
    var (a, b) = (45.0, 65.0)
    val maxDataSample = 0.125
    val agg = "avg"
    
    val ta = System.nanoTime()
    val client: DBestClient = new DBestClient(settings)
    var path = ""
    var tableName = ""
    if (settings.hdfsAvailable) {
      path = s"data/sum_df_${distribution}_label_10m.parquet"
      tableName = s"sum_df_${distribution}_label_10m"
      client.loadHDFSTable(path, tableName)
    } else {
      path = "data/df_sum_uniform_label.parquet"
      tableName = "df_sum_uniform_label"
      client.loadTable(path, tableName)
    }
    logger.info(s"Table: $path")
    val features = Array(distribution)
    val label = "label"

    // Exact counter
    val q1 = s"SELECT ${agg.toUpperCase()}($label) FROM $tableName WHERE ${features(0)} BETWEEN $a AND $b"
    logger.info(q1)
    val res = client.query(q1)
    val t0 = System.nanoTime()
    val exactRes = res.take(1)(0)(0).asInstanceOf[Double]
    logger.info(s"exactCount: $exactRes")
    val t1 = System.nanoTime()
    val elapsedTimeExact = (t1-t0).toLong * 1E-9

    // AQL: Model trained with a eighth of the data
    logger.info("Experiment1 !")
    val (res1, elapsedTime1) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample)

    // AQL: Model trained with a sixteenth of the data
    logger.info("Experiment2 !")
    val (res2, elapsedTime2) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 2)

    // AQL: Model trained with ...
    logger.info("Experiment3 !")
    val (res3, elapsedTime3) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 4)

    // AQL: Model trained with ...
    logger.info("Experiment4 !")
    val (res4, elapsedTime4) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 8)

    // AQL: Model trained with ...
    logger.info("Experiment5 !")
    val (res5, elapsedTime5) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 16)

    // AQL: Model trained with ...
    logger.info("Experiment6 !")
    val (res6, elapsedTime6) = client.queryWithModel(settings, agg, features, label, a, b, maxDataSample / 32)
    val tb = System.nanoTime()

    // Format and write data
    val time = LocalDateTime.now.format(DateTimeFormatter.ofPattern("dd.MM.YYYY / HH:mm"))
    val title = s"""
Report for experiment ($time):
  - table: $tableName
  - aggregation: $agg
  - selectivity: (min: $a), (max: $b)
  - experimentation time: ${tb-ta}\n\n\n
    """
    val aggMode = List("exact", s"$maxDataSample%", s"${maxDataSample/2}%",
        s"${maxDataSample/4}%", s"${maxDataSample/8}%",
        s"${maxDataSample/16}%", s"${maxDataSample/32}%")
    val cv = List(exactRes, res1, res2, res3, res4, res5, res6)
    val timing = List(elapsedTimeExact, elapsedTime1 * 1E-9, elapsedTime2 * 1E-9,
      elapsedTime3 * 1E-9, elapsedTime4 * 1E-9, elapsedTime5 * 1E-9, elapsedTime6 * 1E-9)
    def relativeErrorFun = (realError: Double, approxError: Double) => (approxError - realError) / realError * 100.0
    val relativeError = 0.0 +: cv.tail.map(relativeErrorFun(exactRes, _))

    val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    val outputFile = settings.resultsFolder + s"$tableName" + "_" + s"${a.toInt}" + "_"+ s"${b.toInt}" + "_" + s"$agg" + "_" + timestamp + ".csv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)))
    val header = s"Agg mode\t${agg.toUpperCase()} value\tTime\tRelError(%)\n"
    writer.write(title)
    writer.write(header)
    for ((((c1, c2), c3), c4) <- aggMode zip cv zip timing zip relativeError) {
      val line: String = c1.toString + f"\t$c2%.2f\t$c3%3.2f\t$c4%.2f\t\n"
      writer.write(line)
    }
    writer.close()
    client.close()
  }
}