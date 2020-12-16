package dbest

import java.io._
import DBestClient.DBestClient
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger}


object CountExperiment {
  def main(args: Array[String]) = {
    val logger = Logger.getLogger(this.getClass().getName())

    val resultsFolder = "results/"
    val path = "data/df1m.parquet"
    val tableName = "dataframe1m"
    val client: DBestClient = new DBestClient
    client.loadHDFSTable(path, tableName)

    var (a, b) = (0.0, 100.0)
    val agg = "count"
    val features = Array("col1")

    // Exact counter
    val q1 = s"SELECT ${agg.toUpperCase()}(*) FROM $tableName WHERE ${features(0)} BETWEEN $a AND $b"
    val res = client.query(q1)
    val t0 = System.nanoTime()
    val exactCount = res.take(1)(0)(0).asInstanceOf[Long].toDouble
    logger.info(s"exactCount: $exactCount")
    val t1 = System.nanoTime()
    val elapsedTimeExact = (t1-t0).toLong * 1E-9

    // AQP: Model trained with full data
    val (countFull, elapsedTimeFull) = client.queryWithModel(agg, features, a, b)
    
    // AQL: Model trained with half of the data
    val (countHalf, elapsedTimeHalf) = client.queryWithModel(agg, features, a, b, 0.5)
    
    // AQL: Model trained with quarter of the data
    val (countQuart, elapsedTimeQuart) = client.queryWithModel(agg, features, a, b, 0.25)

  
    // Format and write data
    val aggMode = List("exact", "model 100%", "model 50%", "model 25%")
    val cv = List(exactCount, countFull, countHalf, countQuart)
    val timing = List(elapsedTimeExact, elapsedTimeFull * 1E-9, elapsedTimeHalf * 1E-9, elapsedTimeQuart * 1E-9)


    val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("YYYYMMdd_HHmmss"))
    val outputFile = resultsFolder + s"$tableName" + "_" + s"${a.toInt}" + "_"+ s"${b.toInt}" + "_" + s"$agg" + "_" + timestamp + ".csv"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile)))
    val header = "Agg mode,Count value,Time\n"
    writer.write(header)
    for ((c1, c2, c3) <- (aggMode, cv, timing).zipped) {
      val line: String = c1.toString + f",$c2%.2f,$c3%3.2f\n"
      writer.write(line)
    }
    writer.close()
    client.close()
  }
}