package ml

import org.apache.spark.mllib.stat.KernelDensity;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import traits.DBEstModel
import org.apache.log4j.{Level, Logger}


class SparkKernelDensity(bandWidth: Double = 1.0) extends DBEstModel {
    val logger = Logger.getLogger(this.getClass().getName())
    private var kd = new KernelDensity
    val name = "kernel_density_esitmator"
    
    def getKernelDensity() = kd

    def fit(df: DataFrame, x: Array[String]): SparkKernelDensity = {
        val colRDD = df.select(x.head, x.tail: _*).rdd.map((r: Row) => r.getDouble(0)).cache()
        kd = kd.setSample(colRDD).setBandwidth(bandWidth)
        this
    }

    def fit(rdd: RDD[Double]) = {
        kd = kd.setSample(rdd).setBandwidth(bandWidth)
    }

    def predict(points: Array[Double]) = {
        kd.estimate(points)
    }
}