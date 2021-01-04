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


class QueryEngine(spark: SparkSession, numberTrainingPoint: Int) {
    val logger = Logger.getLogger(this.getClass().getName())

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

        val count = simpson(fD, xMin, xMax, (1 / precision).toInt) * numberTrainingPoint
        val t1 = System.nanoTime()
        (count, t1-t0)
    }
    
    def approxAvg(df: DataFrame,
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

    def approxCount(df: DataFrame, mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double, precision: Double) = {
        val t0 = System.nanoTime()

        val densities = mw.getDensities()
        var count = 0.0
        for ((col, density) <- densities) {
            val minMax = df.agg(F.min(col), F.max(col)).head.toSeq
            val (minimum: Double, maximum: Double) = (minMax(0), minMax(1))
            val ls = linspace(minimum, maximum, density.length).toArray
            val points = (ls zip density)
                            .filter {case (x: Double, va: Double) => x >= xMin && x <= xMax}
                            .map(_._2)
            val h = (xMax - xMin) / (points.length - 1)
            val pointsMap = (for ((i, v) <- (0 until points.length) zip points.reverse) yield xMin + i * h -> v) toMap
            def fD(x: Double) = pointsMap(x)
            count = trapezoid(fD, xMin, xMax, points.length) * numberTrainingPoint
        }

        val t1 = System.nanoTime()
        (count, t1-t0)
    }

    def approxSum(df: DataFrame, mw: ModelWrapper, x: Array[String], y: String, xMin: Double, xMax: Double, precision: Double): (Double, Long) = {
        val t0 = System.nanoTime()

        val kde = mw.getKdeModel().getKernelDensity()
        val reg = mw.getRegModel()

        (0.0, 0L)
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

        val lsDs = ls.toSeq.toDS()
        val regEstimates = reg.transform(input)

        //MUST BE MODIFIED with larger data to support parallelization
        val regArray = regEstimates.select("prediction").rdd.map((r: Row) => r.getDouble(0)).collect()

        val densityRegression = kdeEstimates zip regArray

        def fDR(x: Double) = {
            val (d, r) = densityRegression.head
            d * r
        }

        val sum = simpson(fDR, xMin, xMax, (1 / precision).toInt) * numberTrainingPoint
        val t1 = System.nanoTime()
        (sum, t1-t0)
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