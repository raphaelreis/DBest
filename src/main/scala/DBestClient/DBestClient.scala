package DBestClient

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.spark.sql.DataFrame


class DBestClient(dataset: String) {

    // var df: DataFrame = new DataFrame

    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("DBest client")
      .getOrCreate()
    
    def loadDataset(dataset: String): Unit = {
        val df: DataFrame = spark.read.format("csv")
            .option("header", false)
            .option("delimiter", "|")
            .option("inferSchema", "true")
            .option("mode", "DROPMALFORMED")
            .load(dataset).drop("_c23")
        df.createOrReplaceTempView("store_sales")
    }

    if (Files.exists(Paths.get(dataset))) {
        loadDataset(dataset)
    } else {
        throw new Exception("Dataset does not exist.")
    }

    //sampling
    // val fraction = 0.2
    // val sampler = new Sampler(df)
    // val sampled_df = sampler.uniformSampling(fraction)
    // logger.info("sampled_df.count(): " + sampled_df.count())

    def simpleQuery1() {
        /** Run simple count query with filtering */
        val q2 = "SELECT COUNT(*) FROM store_sales WHERE _c10 BETWEEN 50 AND 100"
        val res2 = spark.sqlContext.sql(q2)
        println("Hello People !")
        spark.time(res2.show())
    }

    def simpleQuery2() {
        /** Run simple average query and variance*/
        val q1 = "SELECT AVG(_c13), VARIANCE(_c13) FROM store_sales"
        val res1 = spark.sqlContext.sql(q1)
        spark.time(res1.show())
    }

}