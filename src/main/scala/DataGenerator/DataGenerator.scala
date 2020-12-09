package DataGenerator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import breeze.stats.distributions.Poisson

object DataGenerator {
  def randomDouble1to100 = scala.util.Random.nextDouble * 100
  def randomPoisson1to100 = Poisson.apply(20).draw()
  // def randomIntTo100 = scala.util.Random.nextInt(100) + 1
  
  def generate1ColTable(spark: SparkSession, nbRow: Int) = {
    import spark.implicits._
    val colNames = Seq.fill(1)("col").zipWithIndex.map{
      case (col, idx) => col + idx.toString()
    }
    // def rowValues = Seq.fill(nbColumn)(randomDouble1to100) 
    // match {
      // case List(a) => (a)
      // case List(a, b) => (a, b)
      // case List(a, b, c) => (a, b, c)
      // case List(a, b, c, d) => (a, b, c, d)
      // case List(a, b, c, d, e) => (a, b, c, d, e)
      // case _ => throw new Exception("More than 5 column generation is not supported")
    // }
    
    val df = spark.sparkContext.parallelize(Seq.fill(nbRow){(randomDouble1to100)}).toDF("col1")
    df
  }

  // def save(df: DataFrame, path: String) = {
  //   df.write.parquet(path)
  // }
}