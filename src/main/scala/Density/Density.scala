package Density

import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.rdd.RDD


// class Density(spark: SparkSession, bandWidth: Double = 1.0) {
class Density(bandWidth: Double = 1.0) {
    val kd = new KernelDensity
    
    // def fit(file: String, delimiter: String, y: String, x: String) {
    //     Dataset<Row> df = spark.read
    // }

    def fit(column: RDD[Double]): KernelDensity = {
        kd.setSample(column).setBandwidth(bandWidth)
    }
}