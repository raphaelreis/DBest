package engine

import breeze.integrate._
import scala.math.exp
import org.apache.spark.mllib.stat.KernelDensity
import breeze.linalg.linspace
import breeze.integrate.trapezoid
import scala.collection.mutable.Stack
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql._
import org.apache.spark.sql.{functions => F} 
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import dbest.ml.SparkKernelDensity
import dbest.ml.LinearRegressor
import dbest.ml.GroupByModelWrapper
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import dbest.ml.ModelWrapper
import traits.Analyser
import scala.collection.mutable.Map
import Sampler.Sampler._


class QueryEngine(spark: SparkSession, var dfSize: Long, var dfMins: Map[String, Double], var dfMaxs: Map[String, Double]) extends Analyser {
    val logger = Logger.getLogger(this.getClass().getName())
    
    def approxCountBySampling(df: DataFrame, x: Array[String], y: String, xMin: Double, xMax: Double, sampleFrac: Double = 1d) = {
        
        val col = x(0)
        val sampleWeight = 1.0 / sampleFrac
        // Get training fraction
        var trainingDF = df
        if (sampleFrac != 1.0) trainingDF = uniformSampling(df, sampleFrac).cache()

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

    def approxCount(df: DataFrame, mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double, precision: Double) = {
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

    def approxSumBySampling(df: DataFrame, x: Array[String], y: String, xMin: Double, xMax: Double, sampleFrac: Double = 1d) = {
        // Get training fraction
        val col = x(0)
        val sampleWeight = 1.0 / sampleFrac
        var trainingDF = df
        if (sampleFrac != 1.0) trainingDF = uniformSampling(df, sampleFrac).cache()

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
    def approxSum(df: DataFrame, mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double, precision: Double): (Double, Long) = {
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

    def approxAvgBySampling(df: DataFrame, x: Array[String], y: String, xMin: Double, xMax: Double, sampleFrac: Double) = {
        // Get training fraction
        var trainingDF = df
        if (sampleFrac != 1.0) trainingDF = uniformSampling(df, sampleFrac)
        
        // Compute Aggregation
        val (count, timeCount) = approxCountBySampling(trainingDF, x, y, xMin, xMax)
        val (sum, timeSum) = approxSumBySampling(trainingDF, x, y, xMin, xMax)
        (sum / count, timeCount + timeSum)
    }

    def approxAvg(df: DataFrame, mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double, precision: Double): (Double, Long) = {
        val (count, time1) = approxCount(df, mw, x, y, xMin, xMax, precision)
        val (sum, time2) = approxSum(df, mw, x, y, xMin, xMax, precision)
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