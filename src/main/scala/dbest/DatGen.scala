package dbest

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import DataGenerator.DataGenerator._



/**
  * USAGE: spark-submit --class dbest.DatGen target/dbest.jar df/df1k.parquet 1000
  */
object DatGen {
  def main(args: Array[String]) = {
    val logger = Logger.getLogger(this.getClass().getName())
    logger.setLevel(Level.DEBUG)
    
    var path = ""
    var tableSize: Int = 100


    val spark: SparkSession = SparkSession.builder
        .appName("DataGen")
        .getOrCreate()
    
    if (args.length == 0) {
      throw new Exception("A file name is required to save the table")
    } else if(args.length == 1) {
      path = args(0)
    } else if(args.length == 2) {
      path = args(0)
      try {
        tableSize = args(1).toInt
      } catch {
        case e: Exception => throw new Exception("Casting error for the argument table size")
      }
    } else {
      throw new Exception("GenDat cannot handle more than 2 arguments")
    }

    /**
      * Generate random table and save it
      */
    logger.info(spark.sparkContext.getConf.getAll.mkString("\n"))
    logger.info("Start data generation")
    val df = generate1ColTable(spark, tableSize).cache()
    logger.info("End data generation")
    // df.show()
    logger.info("Start writing parquet files")
    df.write.parquet(path)
    logger.info("End writing parquet files")
    spark.close()
    }
}