import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import DataGenerator.DataGenerator._

class DataGeneratorTest extends FunSuite {
    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("Data loader")
        .getOrCreate()

    test("TableGenerator.generateTable ") {
        val df = generate1ColTable(spark, 10)
        df.show()
    }

    test("TableGenerator.save ") {
        val df = generate1ColTable(spark, 10)
        save(df, "/Users/Raphael/CS/github.com/DBest/data/df.parquet")
    }
}