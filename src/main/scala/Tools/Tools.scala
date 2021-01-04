package Tools

import Ml.DBestModel
import scala.util.Try
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import java.io._

object makeFileName extends ((String, DBestModel, Array[String], String) => String) {
    def apply(dir: String, model: DBestModel, x: Array[String], y: String): String = {
        dir + model.name + "/" + x.mkString("_") + y
    }
}

object makeDensityFileName {
    def makeDensityFileName(dir: String, df: DataFrame, column: String, trainingFrac: Double) = {
        dir + df.columns.mkString("_") + "_" + column + "_" + trainingFrac.toString() + ".txt"
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