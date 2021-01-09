package QueryEngine

import breeze.integrate._
import scala.math.exp
import org.apache.spark.mllib.stat.KernelDensity
import breeze.linalg._
import breeze.integrate._
import scala.collection.mutable.Stack
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql._
import org.apache.spark.sql.{functions => F} 
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import Ml.SparkKernelDensity
import Ml.LinearRegressor
import Ml.GroupByModelWrapper
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import Ml.ModelWrapper
import traits.Analyser
import scala.collection.mutable.Map


class QueryEngine(spark: SparkSession, var dfSize: Long, var dfMins: Map[String, Double], var dfMaxs: Map[String, Double]) extends Analyser {
    val logger = Logger.getLogger(this.getClass().getName())

    def approxCount(df: DataFrame, mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double, precision: Double) = {
        val t0 = System.nanoTime()

        val densities = mw.getDensities()
        var count = 0.0

        for ((col, density) <- densities) {
            val (minimum, maximum) = (dfMins(col), dfMaxs(col))
            val ls = linspace(minimum, maximum, density.length).toArray
            val selectedDensity = (ls zip density)
                            .filter {case (x: Double, va: Double) => x >= xMin && x <= xMax}
                            .map(_._2)
            logger.info("selectedDensity: ")
            logger.info(selectedDensity.mkString(","))
            val h = (xMax - xMin) / (selectedDensity.length - 1)
            val pointsMap = (for ((i, v) <- (0 until selectedDensity.length) zip selectedDensity) yield xMin + i * h -> v).toMap
            logger.info("pointsMap: ")
            logger.info(pointsMap)
            def fD(x: Double) = if (pointsMap.contains(x)) pointsMap(x) else 0.0
            val nodes = if (selectedDensity.length >= 2) selectedDensity.length else 2 
            count = trapezoid(fD, xMin, xMax, nodes) * dfSize
        }

        val t1 = System.nanoTime()
        (count, t1-t0)
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
        val selectedDensity = (ls zip density)
                                .filter {case (x: Double, va: Double) => x >= xMin && x <= xMax}
                                .map(_._2)
        
        // Make regression points
        import spark.implicits._
        val ds = ls.toSeq.toDF(col)
        val assemblerDataset = new VectorAssembler().setInputCols(Array(col)).setOutputCol("features")
        val input = assemblerDataset.transform(ds)
        val regEstimates = reg.transform(input).select("prediction").rdd.map(_.getDouble(0)).collect()
        val selectedRegressionPred = (ls zip regEstimates)
                                        .filter {case (x: Double, va: Double) => x >= xMin && x <= xMax}
                                        .map(_._2)
        

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

    def approxAvg(df: DataFrame, mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double, precision: Double): (Double, Long) = {
        val (count, time1) = approxCount(df, mw, x, y, xMin, xMax, precision)
        val (sum, time2) = approxSum(df, mw, x, y, xMin, xMax, precision)
        logger.info("count: " + count.toString())
        logger.info("sum: " + sum.toString())
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


/**
  * def approxAvg(df: DataFrame,
            skd: SparkKernelDensity,
            lr: LinearRegressor, 
            x: Array[String], 
            xMin: Double, 
            xMax: Double, 
            precision: Double): (Double, Double) = {

        val t0 = System.nanoTime()

        var kde = skd.getKernelDensity()
        var reg = lr.getLinearRegressionModel()

        import spark.implicits._
        val ls = linspace(xMin, xMax).toArray
        val kdeEstimates = kde.estimate(ls)
        val ds = ls.toSeq.toDS()

        val assemblerDataset = new VectorAssembler().setInputCols(Array("value")).setOutputCol("features")
        val input = assemblerDataset.transform(ds)

        val lsDs = ls.toSeq.toDS()
        val regEstimates = reg.transform(input)

        //MUST BE MODIFIED with larger data to support parallelization
        val regArray = regEstimates.select("prediction").rdd.map((r: Row) => r.getDouble(0)).collect()

        val densityRegression = kdeEstimates zip regArray

        def fDR(x: Double) = {
            val (d, r) = densityRegression.head
            d * r
        }

        def fD = (x: Double) => densityRegression.head._1

        val dR: Double = simpson(fDR, xMin, xMax, (1 / precision).toInt)
        val d: Double = simpson(fD, xMin, xMax, (1 / precision).toInt)

        val t1 = System.nanoTime()

        (dR / d, t1-t0)
    }

    /**
      * @TODO (multi dimensional filtering)
      * def approxCount(xMin: Array[Double], xMax: Array[Double], precision: Double): (Double, Double) = {
      */

    def approxCount(skd: SparkKernelDensity, xMin: Double, xMax: Double,
                     precision: Double) = {
        val t0 = System.nanoTime()

        var kde = skd.getKernelDensity()
        val ls = linspace(xMin, xMax).toArray  
        val kdeEstimates = kde.estimate(ls)
        
        def fD(x: Double): Double = {
            kdeEstimates.head   
        }

        val count = simpson(fD, xMin, xMax, (1 / precision).toInt) * dfSize
        val t1 = System.nanoTime()
        (count, t1-t0)
    }

    def approxSum(df: DataFrame, skd: SparkKernelDensity, lr: LinearRegressor, x: Array[String], xMin: Double, xMax: Double, precision: Double): (Double, Double) = {
        val t0 = System.nanoTime()

        var kde = skd.getKernelDensity()
        var reg = lr.getLinearRegressionModel()

        import spark.implicits._
        val ls = linspace(xMin, xMax).toArray
        val kdeEstimates = kde.estimate(ls)
        val ds = ls.toSeq.toDS()

        val assemblerDataset = new VectorAssembler().setInputCols(Array("value")).setOutputCol("features")
        val input = assemblerDataset.transform(ds)

        val regEstimates = reg.transform(input)

        //MUST BE MODIFIED with larger data to support parallelization
        val regArray = regEstimates.select("prediction").rdd.map((r: Row) => r.getDouble(0)).collect()

        val densityRegression = kdeEstimates zip regArray

        def fDR(x: Double) = {
            val (d, r) = densityRegression.head
            d * r
        }

        val sum = simpson(fDR, xMin, xMax, (1 / precision).toInt) * dfSize
        val t1 = System.nanoTime()
        (sum, t1-t0)
    }
  */