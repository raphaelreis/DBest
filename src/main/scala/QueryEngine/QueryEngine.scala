package QueryEngine

import breeze.integrate._
import scala.math.exp
import org.apache.spark.mllib.stat.KernelDensity
import breeze.linalg._
import breeze.integrate._
import scala.collection.mutable.HashMap
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler

class QueryEngine(spark: SparkSession, kde: KernelDensity, reg: LinearRegressionModel, numberTrainingPoint: Int) {
    
    def approxAvg(df: DataFrame, x: Array[String], xMin: Double, xMax: Double, precision: Double): (Double, Double) = {
        val t0 = System.nanoTime()

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

    def approxSum(df: DataFrame, x: Array[String], xMin: Double, xMax: Double, precision: Double): (Double, Double) = {
        val t0 = System.nanoTime()

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

    /**
      * @TODO (multi dimensional filtering)
      * def approxCount(xMin: Array[Double], xMax: Array[Double], precision: Double): (Double, Double) = {
      */

    def approxCount(xMin: Double, xMax: Double, precision: Double): (Double, Double) = {

        val t0 = System.nanoTime()

        val ls = linspace(xMin, xMax).toArray  
        val kdeEstimates = kde.estimate(ls)

        def fD(x: Double): Double = {
            kdeEstimates.head   
        }

        val count = simpson(fD, xMin, xMax, (1 / precision).toInt) * numberTrainingPoint
        val t1 = System.nanoTime()
        (count, t1-t0)
    }
}
