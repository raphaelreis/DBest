import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.log4j.{Level, Logger}


object DBest {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("Data loader")
      .getOrCreate()
  
    val root = "file:///scratch/ml_aqp/"
    val fileName = "data/sf10/store_sales.dat"

    if (Files.exists(Paths.get(fileName))) {
      logger.info(fileName + " exists")
      
      val df = spark.read.format("csv")
        .option("header", "false")
        .option("delimiter", "|")
        .option("mode", "DROPMALFORMED")
        .load(root + fileName)

      df.show()
    } else {
      logger.info(fileName + " does not exist !")
    }
    
    logger.info("stopping spark...")
    spark.stop()
    logger.info("spark stopped")
  }
}