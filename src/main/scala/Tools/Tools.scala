package tools

import math.round
import scala.util.Try
import org.apache.spark.sql.DataFrame
import java.io.{File, FileWriter, BufferedWriter}

import traits.DBEstModel


object makeFileName {
    def makeFileName(dir: String, df: DataFrame,  model: DBEstModel, x: Array[String], y: String, trainingFrac: Double): String = {
        dir + model.name + "_" + df.columns.mkString + x.mkString("_") + y + "_" + trainingFrac.toString()
    }
}

object makeDensityFileName {
    def makeDensityFileName(dir: String, df: DataFrame, column: String, evalSpacing: Double, trainingFrac: Double) = {
        val roundSpace = math.round(evalSpacing * 100).toDouble / 100
        dir + df.columns.mkString("_") + "_" + column + "_spacing" + roundSpace.toString + "_TF" + trainingFrac.toString() + ".txt"
    }
}

object hasColumn extends ((DataFrame, String) => Boolean) {
    def apply(df: DataFrame, col: String): Boolean = {
        Try(df(col)).isSuccess
    }
}
        
object computeColumnUniqueValues extends ((DataFrame, String) => Array[Any]) {
    def apply(df: DataFrame, col: String): Array[Any] = {
        df.select(col).rdd.map(r => r.get(0)).distinct.collect().toArray
    }
}

object fileWriter {
    def writeFile(filename: String, lines: Array[Double]): Unit = {
        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        for (line <- lines) {
            bw.write(line.toString() + " ")
        }
        bw.close()
    }
}