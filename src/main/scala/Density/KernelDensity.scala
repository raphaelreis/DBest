package Density

import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext


class Density(spark: SparkSession, bandWidth: Double = 1.0) {
    val kd = new KernelDensity
    
    def fit(file: String, delimiter: String, y: String, x: String) {
        val df: Dataset[Row]  = spark.read
            .format("csv")
            .option("header", "false")
            .option("delimiter", delimiter)
            .load(file)

        df.withColumn(x, df.col(x).cast("double"))

        val sc = spark.sparkContext
        val collection: Array[Double] = df.select(x).collect().toArray.map(row => row.asInstanceOf[Double])
    }

    def fit(column: RDD[Double]) = {
        kd.setSample(column).setBandwidth(bandWidth)
    }

    def predict(point: Array[Double]) = {
        println("kernelDensity.predict not implemented")
    }
}