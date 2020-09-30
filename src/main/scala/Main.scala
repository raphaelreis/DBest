import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import java.nio.file.{Paths, Files}
import org.apache.log4j.{Level, Logger}
import Sampler._
import org.apache.spark.sql.functions._


object DBest {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("Data loader")
      .getOrCreate()
  
    // val root = "file:///scratch/ml_aqp/"
    // val fileName = "data/sf10/store_sales.dat"
    val root = ""
    val fileName = "data/store_sales_sample.dat"
    if (Files.exists(Paths.get(fileName))) {
      logger.info(fileName + " exists")

      val df = spark.read.format("csv")
        .option("header", false)
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .load(root + fileName).drop("_c23")
      df.createOrReplaceTempView("store_sales")

      //sampling
      val fraction = 0.2
      val sampler = new Sampler(df)
      val sampled_df = sampler.uniformSampling(fraction)
      logger.info("sampled_df.count(): " + sampled_df.count())

      //Queries
      //1)
      val q1 = "SELECT AVG(_c13), VARIANCE(_c13) FROM store_sales"
      val res1 = spark.sqlContext.sql(q1)
      spark.time(res1.show())
      
      

      // logger.info(s"t10: $t10 | t11: $t11 | t20: $t20 | t21: $t21 | t30: $t30 | t31: $t31")
      // logger.info(s"t10: $t10 | t11: $t11")
      // logger.info(s"res1: $res1 | res1Sample: $res1Sample")

    } else {
      logger.info(fileName + " does not exist !")
    }
    
    logger.info("stopping spark...")
    spark.stop()
    logger.info("spark stopped")
  }
}