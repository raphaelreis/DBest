package DataLoader

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.{functions=>F}

class DataLoader {
    val logger = Logger.getLogger(this.getClass().getName())

    def loadTable(spark: SparkSession, path: String, tableName: String, 
                    format: String = "parquet", header: Boolean = false, 
                    delimiter: String = "|", inferSchema: Boolean = true, 
                    mode: String = "DROPMALFORMED") = {
        
        var df = spark.read.format(format)
            .option("header", header)
            .option("delimiter", delimiter)
            .option("inferSchema", inferSchema)
            .option("mode", mode)
            .load("file:///" + path)

        //Cleaning
        val nullCounts = df.select(df.columns.map(c => F.sum(F.col(c).isNull.cast("int")).alias(c)): _*).collect()(0)
        val mapNulls = nullCounts.getValuesMap[Any](nullCounts.schema.fieldNames)
        val mapFullNulls = mapNulls.filter{case (_: String, v: Any) => v == df.count()}
        df = df.drop(mapFullNulls.keys.toSeq: _*).na.drop
        df
    }
}
