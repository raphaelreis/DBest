package DataLoader

import org.apache.spark.sql._

class DataLoader(spark: SparkSession) {
    def loadTable(table: String): DataFrame = {
        val df = spark.read.format("csv")
            .option("header", false)
            .option("delimiter", "|")
            .option("inferSchema", "true")
            .option("mode", "DROPMALFORMED")
            .load(table).drop("_c23")
            .na.drop().cache()
        df.createOrReplaceTempView("store_sales")
        df
    }
    def load() = {
        throw ???
    }
}
