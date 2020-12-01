package Ml

import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import Tools._

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
    private var groupValues = Array[Any]()
    private var regModels = Map[Any, LinearRegressor]()
    private var kdeModels = Map[String, Map[Any,SparkKernelDensity]]()

    def getRegModels() = {
        if (regModels.isEmpty) throw new Exception("Map of regression models is empty")
        regModels
    }

    def getKdeModels() = {
        if (kdeModels.isEmpty) throw new Exception("Map of kernel density models is empty")
        kdeModels
    }

    def fitRegs(df: DataFrame, groupColumn: String) = {
        if (groupValues.isEmpty) {groupValues = computeColumnUniqueValues(df, groupColumn)}
        for (gVal <- groupValues) {
            var groupDF = df.filter(r => r.getInt(0) == gVal)
            if (!groupDF.isEmpty) {
                val lr = new LinearRegressor()
                lr.fit(groupDF)
                regModels += gVal -> lr
            }
        }
    }

    def fitDensities(mapRDD: Map[String, RDD[(Any, Double)]]) = {
        if (groupValues.isEmpty) throw new Exception("groupValues is not computed")
        for (col <- mapRDD.keys) {
            val columnGroupMap = mapRDD(col)
            if (!kdeModels.contains(col)) kdeModels += col -> Map[Any, SparkKernelDensity]()
            for (gVal <- groupValues) {
                val kde = new SparkKernelDensity(kernelBandeWidth)
                val gValRDD = columnGroupMap.filter{
                    case ((groupVal, _)) => groupVal == gVal
                }.map{
                    case ((_, value)) => value
                }
                kde.fit(gValRDD)
                kdeModels(col) += gVal -> kde
            }
        }
    }

    def fit(dp: DataProcessor.DataProcessor, groupColumn: String) = {
        val processedDF = dp.getPreprocessedDF()
        val groupByMapRDD = dp.getGroupByMapRDD()
        fitRegs(processedDF, groupColumn)
        fitDensities(groupByMapRDD)
    }
}