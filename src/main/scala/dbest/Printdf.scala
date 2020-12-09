package dbest

import org.apache.spark.sql.SparkSession

object Printdf {
  def main(args: Array[String]) = {

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("PrintDfFromParquet")
        .getOrCreate()

    if (args.length == 0) {
      throw new Exception("You must give the path for the dataframe")
    }
    spark.read.parquet(args(0)).show()
    spark.close()
  }
}