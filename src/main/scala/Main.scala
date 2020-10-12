
import org.apache.log4j.{Level, Logger}
import DBestClient._
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD
import breeze.integrate._


object DBest {
  def main(args: Array[String]): Unit = {

    val logger = Logger.getLogger(this.getClass().getName())

    // val root = "file:///scratch/ml_aqp/"
    // val fileName = "data/sf10/store_sales.dat"
    val root = ""
    val fileName = "data/store_sales_sample.dat"

    var client: DBestClient = new DBestClient(root + fileName)
    // client.simpleQuery1()
    client.simpleQuery1WithModel()

  }
}