package Ml

import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD

class ModelWrapper(kernelBandeWidth: Double = 3.0) {

    private val logger = Logger.getLogger(this.getClass().getName())
    private var reg = new LinearRegressor()
    private var density = new SparkKernelDensity(kernelBandeWidth)

    def fitReg(df: DataFrame, x: Array[String], y: String) = {
        reg.fit(df, x, y)
    }

    def fitDensity(df: DataFrame, x: Array[String]) = {
        density.fit(df, x)
    }

    def fitOrLoad(df: DataFrame, x: Array[String], y: String) = {
        
        //fit kernel density
        density = fitDensity(df, x)
        
        val mio = new ModelIO(x, y)
    
        //load or fit regression 
        if (mio.exists(reg)) {
            logger.info("regression model exists and is loaded")
            reg = mio.readModel(reg).asInstanceOf[LinearRegressor]
        } else {
            logger.info("regression does not exists and will be fit")
            fitReg(df, x, y)
            mio.writeModel(reg)
        }
    }
}

class GroupByModelWrapper(kernelBandeWidth: Double = 3.0) {
    private val logger = Logger.getLogger(this.getClass().getName())
    var regModels = Map[Any, LinearRegressor]()
    var kdeModels = Map[Any, SparkKernelDensity]()
    var keys = Array[Any]()

    private def computeKeys(df: DataFrame, groupCol: String) = {
        df.select(groupCol).rdd.map(r => r.get(0)).distinct.collect().toArray
    }

    // private def preprocessDf()

    def fitRegs(df: DataFrame, groupCol: String) = {
        
        if (keys.isEmpty) {
            keys = computeKeys(df, groupCol)
        }
        for (key <- keys) {
            var groupDF = df.filter(r => r.getInt(0) == key)
            if (!groupDF.isEmpty) {
                val lr = new LinearRegressor()
                lr.fit(groupDF)
                regModels += key -> lr
            }
        }
    }

    def fitDensities(mapRDD: Map[String, RDD[Double]]) = {
        for (col <- mapRDD.keys) {
            val kde = new SparkKernelDensity
            kde.fit(mapRDD(col))
            kdeModels
        }
    }
}