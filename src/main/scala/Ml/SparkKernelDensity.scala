package Ml

import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext


class SparkKernelDensity(bandWidth: Double = 1.0) extends DBestModel {
    
    private var kd = new KernelDensity
    val name = "kd"
    
    def getKernelDensity() = kd

    def fit(df: DataFrame, x: Array[String]): SparkKernelDensity = {
        kd = kd.setSample(df.select(x.head, x.tail: _*).rdd.map((r: Row) => r.getDouble(0))).setBandwidth(bandWidth)
        this
    }

    def predict(points: Array[Double]) = {
        kd.estimate(points)
    }
}