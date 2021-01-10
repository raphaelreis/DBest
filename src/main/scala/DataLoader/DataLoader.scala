package DataLoader

import org.apache.spark.sql._

class DataLoader {
    def loadTable(spark: SparkSession, path: String, tableName: String, 
                    format: String = "parquet", header: Boolean = false, 
                    delimiter: String = "|", inferSchema: Boolean = true, 
                    mode: String = "DROPMALFORMED") = {

        val df = spark.read.format(format)
            .option("header", header)
            .option("delimiter", delimiter)
            .option("inferSchema", inferSchema)
            .option("mode", mode)
            .load(path)
            .na.drop()
        df.createOrReplaceTempView(tableName)
        df
    }
}
