import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import sampler.Sampler._

class SamplerTest extends AnyFunSuite {
    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("Data loader")
        .getOrCreate()

    val fileName = "data/store_sales_sample.dat"
    val df = spark.read.format("csv")
        .option("header", false)
        .option("delimiter", "|")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .load(fileName).drop("_c23")

    test("sampler.uniformSampling") {
        val fraction = 0.2
        // val sampler = new sampler.Sampler(df)
        val sampled_df = uniformSampling(df, fraction)
        logger.info("df count: " + df.count())
        logger.info("sampled_count: " + sampled_df.count())

        val frac = sampled_df.count().toDouble / df.count()  
        val err = 0.01
        assert((frac < fraction + err) && (frac > fraction - err))

    }
}