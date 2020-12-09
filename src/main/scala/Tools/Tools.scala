package Tools

import Ml.DBestModel
import scala.util.Try
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object makeFileName extends ((DBestModel, Array[String], String) => String) {
    def apply(model: DBestModel, x: Array[String], y: String): String = {
        val root = "/Users/Raphael/CS/github.com/DBest/src/main/resources/models/"
        root + model.name + "/" + x.mkString("_") + y
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