package engine

import org.apache.log4j.Logger
import scala.math.exp
import scala.collection.mutable.Map
import breeze.linalg.linspace
import breeze.integrate.{trapezoid, simpson}
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.{SparkSession, DataFrame, functions => F} 

import traits.Analyser
import dbest.ml.ModelWrapper
import dbest.ml.LinearRegressor
import dbest.ml.GroupByModelWrapper
import sampler.Sampler.uniformSampling

class QueryEngine(spark: SparkSession) extends Analyser {
    val logger = Logger.getLogger(this.getClass().getName())

    var dfSize: Long = _
    var dfMins: Map[String, Double] = _
    var dfMaxs: Map[String, Double] = _
    var trainingDF: DataFrame = _
    var sampleFrac = 1.0 
    def sampleWeight = 1.0 / sampleFrac

    def setNewTrainingDF(newTrainingDF: DataFrame, samplingFrac: Double) {
        sampleFrac = samplingFrac
        trainingDF = newTrainingDF
        if (!trainingDF.storageLevel.useMemory)
            throw new Exception("Training DataFrame from Query Engine is not in Memory")
    }

    def setStats(size: Long, mins: Map[String, Double], maxs: Map[String, Double]) {
        dfSize = size; dfMins = mins; dfMaxs = maxs
    }

    def approxCountBySampling(x: Array[String], y: String, xMin: Double, xMax: Double) = {
        val col = x(0)
        // Compute Aggregation
        val t0 = System.nanoTime()
        val count = trainingDF.select(col, y).rdd.mapPartitions {
                iter => {
                    if (!iter.isEmpty) {
                            Array(iter.filter{case r => {val att = r.getDouble(0); att >= xMin && att <= xMax}}
                                .map(_ => 1.0).foldLeft(0.0)(_+_)).toIterator
                    } else Array(0.0).toIterator
                    
                }
        }.reduce(_+_).toDouble * sampleWeight
        val t1 = System.nanoTime()
        (count, t1-t0)
    }

    def approxCount(mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double) = {
        val t0 = System.nanoTime()

        val densities = mw.getDensities()
        var count = 0.0

        for ((col, density) <- densities) {
            val (minimum, maximum) = (dfMins(col), dfMaxs(col))
            val ls = linspace(minimum, maximum, density.length).toArray
            var selectedDensity = (ls zip density)
                            .filter {case (x: Double, va: Double) => x >= xMin && x <= xMax}
                            .map(_._2)
            
            if (selectedDensity.length < 5) {
                selectedDensity = (ls zip density)
                            .filter {case (x: Double, va: Double) => x >= xMin - 0.5 && x <= xMax + 0.5}
                            .map(_._2)
            }
            val h = (xMax - xMin) / (selectedDensity.length - 1)
            val pointsMap = (for ((i, v) <- (0 until selectedDensity.length) zip selectedDensity) yield xMin + i * h -> v).toMap
            def fD(x: Double) = if (pointsMap.contains(x)) pointsMap(x) else 0.0
            val nodes = if (selectedDensity.length >= 2) selectedDensity.length else 2 
            count = trapezoid(fD, xMin, xMax, nodes) * dfSize
        }

        val t1 = System.nanoTime()
        (count, t1-t0)
    }

    def approxSumBySampling(x: Array[String], y: String, xMin: Double, xMax: Double) = {
        val col = x(0)
        // Compute Aggregation
        val t0 = System.nanoTime()
        val sum = trainingDF.select(col, y).rdd.mapPartitions {
                iter => {
                        if (!iter.isEmpty) {
                            Array(iter.filter{case r => {val att = r.getDouble(0); att >= xMin && att <= xMax}}
                                    .map(_.getDouble(1)).foldLeft(0.0)(_+_)).toIterator
                        } else Array(0.0).toIterator
                }
        }.reduce(_+_) * sampleWeight
        val t1 = System.nanoTime()

        (sum, t1-t0)
    }

    // Only support univariate selection
    def approxSum(mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double): (Double, Long) = {
        val t0 = System.nanoTime()

        val col = x(0)
        val densities = mw.getDensities()
        val density = densities(col)
        val reg = mw.getRegModel()
        
        // Linear space building
        val (minimum, maximum) = (dfMins(col), dfMaxs(col))
        val ls = linspace(minimum, maximum, density.length).toArray
        
        // Make density points
        var selectedDensity = (ls zip density)
                                .filter {case (x: Double, va: Double) => x >= xMin && x <= xMax}
                                .map(_._2)
        
        // Make regression points
        import spark.implicits._
        val ds = ls.toSeq.toDF(col)
        val assemblerDataset = new VectorAssembler().setInputCols(Array(col)).setOutputCol("features")
        val input = assemblerDataset.transform(ds)
        val regEstimates = reg.transform(input).select("prediction").rdd.map(_.getDouble(0)).collect()
        var selectedRegressionPred = (ls zip regEstimates)
                                        .filter {case (x: Double, va: Double) => x >= xMin && x <= xMax}
                                        .map(_._2)

        if (selectedDensity.length < 5) {
                selectedDensity = (ls zip density)
                            .filter {case (x: Double, va: Double) => x >= xMin - 0.5 && x <= xMax + 0.5}
                            .map(_._2)
                selectedRegressionPred = (ls zip regEstimates)
                                        .filter {case (x: Double, va: Double) => x >= xMin - 0.5 && x <= xMax + 0.5}
                                        .map(_._2)
            }

        // Mapping for integration
        val h = (xMax - xMin) / (selectedDensity.length - 1)
        val sumPointsMap = (
            for (((i, densityPoint), regressionPoint) <- (0 until selectedDensity.length) zip selectedDensity zip selectedRegressionPred)
             yield xMin + i * h -> densityPoint * regressionPoint
            ) toMap

        // Integration
        def sumfD(x: Double) = if (sumPointsMap.contains(x)) sumPointsMap(x) else 0.0
        val nodes = if (selectedDensity.length >= 2) selectedDensity.length else 2 
        val sum = trapezoid(sumfD, xMin, xMax, nodes) * dfSize

        val t1 = System.nanoTime()
        (sum, t1-t0)
    }

    def approxAvgBySampling(x: Array[String], y: String, xMin: Double, xMax: Double) = {
        // Compute Aggregation
        val (count, timeCount) = approxCountBySampling(x, y, xMin, xMax)
        val (sum, timeSum) = approxSumBySampling(x, y, xMin, xMax)
        (sum / count, timeCount + timeSum)
    }

    def approxAvg(mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double): (Double, Long) = {
        val (count, time1) = approxCount(mw, x, y, xMin, xMax)
        val (sum, time2) = approxSum(mw, x, y, xMin, xMax)
        (sum / count, time1 + time2)
    }

    def groupByApproxCount(gmw: GroupByModelWrapper, groupValues: Array[(Any, Int)], groupColumn: String, 
        features: Array[String], xMin: Double, xMax: Double, precision: Double) = {
        val t0 = System.nanoTime()
        
        val kdeMap = gmw.getKdeModels()
        var countPerGroupValue = Array[(String, Any, Double)]()
        for (col <- features) {
            val columnModels = kdeMap(col)
            for ((gVal, pointsNumber) <- groupValues) {
                val gValModel = columnModels(gVal)
                val kdeEstimates = gValModel.predict(linspace(xMin, xMax).toArray)
                val count = simpson(_ => kdeEstimates.head, xMin, xMax, (1 / precision).toInt) * pointsNumber
                logger.info(s"Count value for groupId $gVal: $count")
                countPerGroupValue :+= (col, gVal, count)
            }
        }

        val ret = countPerGroupValue.groupBy(_._1).mapValues(_ map {
            case (_, any, double) => (any, double)
        })
        val t1 = System.nanoTime()
        (ret, t1-t0)
    }
}