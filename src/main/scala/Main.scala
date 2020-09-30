import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import java.nio.file.{Paths, Files}
import org.apache.log4j.{Level, Logger}
import Sampler._


object DBest {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("Data loader")
      .getOrCreate()
  
    // val root = "file:///scratch/ml_aqp/"
    // val fileName = "data/sf10/store_sales.dat"

    if (Files.exists(Paths.get(fileName))) {
      logger.info(fileName + " exists")

      val df = spark.read.format("csv")
        .option("header", false)
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .load(root + fileName).drop("_c23")

      //sampling
      val fraction = 0.2
      val sampler = new Sampler(df)
      val sampled_df = sampler.uniformSampling(fraction)
      logger.info("sampled_df.count(): " + sampled_df.count())

      //Queries
      //1)
      val res1 = sampled_df.select("_c13")
      //2)
      val res2 = sampled_df.where("_c10 BETWEEN 50 AND 100").count()
      //3)
      val res3 = sampled_df.select("_c0", "_c20").groupBy("_c0").sum("_c20")

      //Metrics
    } else {
      logger.info(fileName + " does not exist !")
    }
    
    logger.info("stopping spark...")
    spark.stop()
    logger.info("spark stopped")
  }
}