package QueryEngine

import breeze.integrate._
import scala.math.exp
import org.apache.spark.mllib.stat.KernelDensity
import breeze.linalg._
import breeze.integrate._
import scala.collection.mutable.HashMap

class QueryEngine(kde: KernelDensity, numberTrainingPoint: Int) {
    



    def approxAvg(xMin: Double, xMax: Double) = {
        throw ???
    }

    def approxSum(xMin: Double, xMax: Double) = {
        throw ???
    }

    def approxCount(xMin: Double, xMax: Double, precision: Double): (Double, Double) = {

        val t0 = System.nanoTime()

        // def f(x: Double): Double = {
        //     //Not optimal, array allocation (estimate of one value) or implement own simpson integral
        //     kde.estimate(Array(x))(0)
        // }


        val ls = linspace(xMin, xMax).toArray  
        val kdeEstimates = kde.estimate(ls)
        def fBetter(x: Double): Double = {
            kdeEstimates.head   
        }

        val count = simpson(fBetter, xMin, xMax, (1 / precision).toInt) * numberTrainingPoint
        val t1 = System.nanoTime()
        (count, t1-t0)
    }
}
