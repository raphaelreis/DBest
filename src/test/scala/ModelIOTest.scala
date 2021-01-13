import dbest.ml._
import DataLoader._
import java.io.File
import settings.Settings
import client.DBestClient
import java.nio.file.{Paths, Files}
import org.apache.log4j.{Level, Logger}
import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.{functions=>F}
import org.apache.spark.sql.types.DoubleType


class ModelIOTest extends AnyFunSuite {
    // Init settings and logger
    val appName = "ModelIO test"
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

    

    test("ModelIO test model writting xgboost regressor") {
        val spark = SparkSession.builder()
            .master("local[*]")
            .getOrCreate()
        val df = spark.read
            .option("delimiter", "|")
            .csv(settings.baseDir + "data/store_sales_sample.dat")
            .withColumn("ss_wholesale_cost", F.col("_c11").cast(DoubleType))
            .withColumn("ss_list_price", F.col("_c12").cast(DoubleType))
            .select("ss_list_price", "ss_wholesale_cost")
            .cache()

        val features = Array("ss_list_price")
        val label = "ss_wholesale_cost"
        val dp = new dbest.dataprocessor()

        val model = new DBEstXGBoostRegressor()

        val dp = new dbest.dataprocessor.DataProcessor()
        
        spark.close()
    }

    
}