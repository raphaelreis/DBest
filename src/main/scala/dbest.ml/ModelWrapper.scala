package dbest.ml

import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import tools._
import tools.fileWriter.writeFile
import tools.makeDensityFileName.makeDensityFileName
import breeze.linalg._
import java.nio.file.{Paths, Files}
import scala.io._
import sampler.Sampler._
import settings.Settings
import traits.Analyser
import org.apache.spark.ml.tuning.CrossValidatorModel

class ModelWrapper(settings: Settings, var dfSize: Long, var dfMins: Map[String, Double], var dfMaxs: Map[String, Double]) extends Analyser {

    private val logger = Logger.getLogger(this.getClass().getName())
    private var reg = if(settings.modelType == "linear") new LinearRegressor() else new DBEstXGBoostRegressor()
    private var kde = new SparkKernelDensity(settings.defaultKernelBandWidth)
    private var densities = Map[String, Array[Double]]()

    def getRegModel() = reg
    def getKdeModel() = kde
    def getDensities() = densities

    def fitDensity(df: DataFrame, x: Array[String]) = {
        kde.fit(df, x)
    }

    def saveDensities(df: DataFrame, x: Array[String], evalSpacing: Double, trainingFrac: Double) = {
        if (!x.isEmpty) {
            logger.info("Saving densities")
            val col = x(0)
            val density = new SparkKernelDensity(settings.defaultKernelBandWidth)
            val colRDD = df.select(col).rdd.map(_.getDouble(0)).cache()
            density.fit(colRDD)
            val (minimum, maximum) = (dfMins(col), dfMaxs(col))
            val precision = ((maximum - minimum) / evalSpacing).toInt
            val linsp = linspace(minimum, maximum, precision).toArray
            val estimates = density.predict(linsp)
            val fileName = makeDensityFileName(settings.dpath, df.drop("features", "label"), col, evalSpacing, trainingFrac)
            writeFile(fileName, estimates)
        
        }
    }

    def loadDensities(df: DataFrame, x: Array[String], evalSpacing: Double, trainingFrac: Double) = {
        for (col <- x) {
            val fileName = makeDensityFileName(settings.dpath, df.drop("features", "label"), col, evalSpacing, trainingFrac)
            if (Files.exists(Paths.get(fileName))) {
                val fileSource = Source.fromFile(fileName)  
                for(line <- fileSource.getLines){  
                    densities += col -> line.split(" ").map(_.toDouble)
                }   
                fileSource.close() 
            }
        }
    }
 
    def fitOrLoad(aggFun: String, df: DataFrame, x: Array[String], y: String, trainingFrac: Double) {
        
        val densityEvaluationSpacing = settings.densitiyInterspacEvaluation

        var trainingDF = df
        // Get training fraction
        if (trainingFrac != 1.0) trainingDF = uniformSampling(df, trainingFrac)
        trainingDF = trainingDF.cache()
        // Save densities if not registered
        var unknownColumns = Array[String]()
        for (col <- x) {
            val path = makeDensityFileName(settings.dpath, df.drop("features", "label"), col, densityEvaluationSpacing, trainingFrac)
            if (!Files.exists(Paths.get(path))) 
                unknownColumns = unknownColumns :+ col
        }
        saveDensities(trainingDF, unknownColumns, densityEvaluationSpacing, trainingFrac)

        // Load all saved densities
        loadDensities(df, x, densityEvaluationSpacing, trainingFrac)
        
        // Fit or load regression
        if (aggFun != "count") {
            val mio = new ModelIO(settings.rpath, df.drop("features", "label"), x, y, trainingFrac)
            //load or fit regression 
            if (mio.exists(reg)) {
                logger.info("regression model exists and is loaded")
                reg = if(settings.modelType == "linear") mio.readModel(reg).asInstanceOf[LinearRegressor] 
                        else mio.readModel(reg).asInstanceOf[DBEstXGBoostRegressor]
            } else {
                logger.info("regression does not exists and will be fit")
                reg.crossValidate(trainingDF, settings.crossValNumFolds, settings.numWorkers)
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

    def fit(dp: dbest.dataprocessor.DataProcessor, groupColumn: String) = {
        val processedDF = dp.getPreprocessedDF()
        val groupByMapRDD = dp.getGroupByMapRDD()
        fitRegs(processedDF, groupColumn)
        fitDensities(groupByMapRDD)
    }
}