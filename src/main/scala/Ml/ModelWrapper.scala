package Ml

import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import Tools._
import Tools.fileWriter.writeFile
import Tools.makeDensityFileName.makeDensityFileName
import breeze.linalg._
import java.nio.file.{Paths, Files}
import scala.io._
import Sampler.Sampler._
import settings.Settings

class ModelWrapper(settings: Settings, kernelBandeWidth: Double = 3.0) {

    private val logger = Logger.getLogger(this.getClass().getName())
    private var reg = new LinearRegressor()
    private var kde = new SparkKernelDensity(kernelBandeWidth)
    private var densities = Map[String, Array[Double]]()

    def getRegModel() = reg
    def getKdeModel() = kde
    def getDensities() = densities

    def fitReg(df: DataFrame) = {
        reg.fit(df)
    }
    def fitReg(df: DataFrame, x: Array[String], y: String) = {
        reg.fit(df, x, y)
    }

    def fitDensity(df: DataFrame, x: Array[String]) = {
        kde.fit(df, x)
    }

    def saveDensities(df: DataFrame, x: Array[String], trainingFrac: Double) = {
        for (col <- x) {
            val density = new SparkKernelDensity(kernelBandeWidth)
            val colRDD = df.select(col).rdd.map((r: Row) => r.getDouble(0)).cache()
            density.fit(colRDD)
            val (min, max) = (colRDD.min(), colRDD.max())
            val linsp = linspace(min, max).toArray
            val estimates = density.predict(linsp)
            val fileName = makeDensityFileName(settings.dpath, df, col, trainingFrac)
            writeFile(fileName, estimates)
        }
    }

    def loadDensities(df: DataFrame, x: Array[String], trainingFrac: Double) = {
        for (col <- x) {
            val fileName = makeDensityFileName(settings.dpath, df, col, trainingFrac)
            if (Files.exists(Paths.get(fileName))) {
                val fileSource = Source.fromFile(fileName)  
                for(line <- fileSource.getLines){  
                    densities += col -> line.split(" ").map(_.toDouble)
                }   
                fileSource.close() 
            }
            logger.info("Model used: " + fileName)
        }
    }

    def fitOrLoad(aggFun: String, df: DataFrame, x: Array[String], y: String, trainingFrac: Double) {
        
        var trainingDF = df
        // Get training fraction
        if (trainingFrac != 1.0) trainingDF = uniformSampling(df, trainingFrac)
        
        // Save densities if not registered
        var unknownColumns = Array[String]()
        for (col <- x) {
            val path = makeDensityFileName(settings.dpath, df, col, trainingFrac)
            if (!Files.exists(Paths.get(path))) 
                unknownColumns = unknownColumns :+ col
        }
        saveDensities(trainingDF, unknownColumns, trainingFrac)

        // Load all saved densities
        loadDensities(df, x, trainingFrac)
        
        // Fit or load regression
        if (aggFun != "count") {
            val mio = new ModelIO(settings.rpath, x, y)
            //load or fit regression 
            if (mio.exists(reg)) {
                logger.info("regression model exists and is loaded")
                reg = mio.readModel(reg).asInstanceOf[LinearRegressor]
            } else {
                logger.info("regression does not exists and will be fit")
                fitReg(df)
                mio.writeModel(reg)
            }
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