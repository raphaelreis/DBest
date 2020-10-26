package Ml

import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

class ModelWrapper(kernelBandeWidth: Double = 3.0) {

    private val logger = Logger.getLogger(this.getClass().getName())
    private var reg = new LinearRegressor()
    private var density = new SparkKernelDensity(kernelBandeWidth)

    private def fitReg(df: DataFrame, x: Array[String], y: String) = {
        reg.fit(df, x, y)
    }

    private def fitDensity(df: DataFrame, x: Array[String]) = {
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