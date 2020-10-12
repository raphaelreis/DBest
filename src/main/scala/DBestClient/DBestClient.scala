package DBestClient

import org.apache.spark.sql.SparkSession
import java.nio.file.{Paths, Files}
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.stat.KernelDensity
import Ml._
import QueryEngine._


class DBestClient(dataset: String) {

    var df: DataFrame = _

    val spark: SparkSession = SparkSession.builder
      .master("local")
      .appName("DBest client")
      .getOrCreate()
    
    def loadDataset(dataset: String): Unit = {
        df = spark.read.format("csv")
            .option("header", false)
            .option("delimiter", "|")
            .option("inferSchema", "true")
            .option("mode", "DROPMALFORMED")
            .load(dataset).drop("_c23")
        df.createOrReplaceTempView("store_sales")
        df = df.na.drop().cache()
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
        val q2 = "SELECT COUNT(*) FROM store_sales WHERE _c12 BETWEEN 50 AND 100"
        val res2 = spark.sqlContext.sql(q2)
        println("Hello People !")
        spark.time(res2.show())
    }

    def simpleQuery1WithModel() {
        /**
          * Same as simpleQuery1 but with AQP
          */
        val d = new SparkKernelDensity(3.0)
        val kde = d.fit(df, "_c12")
        val qe = new QueryEngine(kde, df.count().toInt)

        val (count, elipseTime) = qe.approxCount(50, 100, 0.01)

        println(s"Count value with model: $count")
        println(s"Time to compute count: $elipseTime")

    }

    def simpleQuery2() {
        /** Run simple average query and variance*/
        val q1 = "SELECT AVG(_c13), VARIANCE(_c13) FROM store_sales"
        val res1 = spark.sqlContext.sql(q1)
        spark.time(res1.show())
    }

}