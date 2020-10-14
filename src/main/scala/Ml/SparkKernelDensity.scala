package Ml

import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext


class SparkKernelDensity(bandWidth: Double = 1.0) {
    val kd = new KernelDensity
    
    // def fit(file: String, delimiter: String, y: String, x: String) {
    //     val df: Dataset[Row]  = spark.read
    //         .format("csv")
    //         .option("header", "false")
    //         .option("delimiter", delimiter)
    //         .load(file)

    //     df.withColumn(x, df.col(x).cast("double"))

    //     val sc = spark.sparkContext
    //     val collection: Array[Double] = df.select(x).collect().toArray.map(row => row.asInstanceOf[Double])
    // }

    def fit(df: DataFrame, x: Array[String]): KernelDensity = {
        kd.setSample(df.select(x.head, x.tail: _*).rdd.map((r: Row) => r.getDouble(0))).setBandwidth(bandWidth)
    }

    def predict(point: Array[Double]) = {
        throw ???
    }
}