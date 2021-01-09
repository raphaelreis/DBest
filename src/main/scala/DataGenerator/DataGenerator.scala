package DataGenerator

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import breeze.stats.distributions.Beta
import org.apache.spark.sql.functions.{rand, randn}



object DataGenerator {
  
  def generate(spark: SparkSession, nbRow: Int, distribution: String, alpha: Double = 2.0,
                beta: Double = 5.0) = distribution match {
    case "u" => generateOneColumnUniform(spark, nbRow)
    case "g" => generateOneColumnGaussian(spark, nbRow)
    case "b" => generateBeta1ColTable(spark, nbRow, alpha, beta)
    case _ => throw new NotImplementedError("DataGenerator supports only Unform 'u' and Gaussian as 'g' processes.")
  }
  
  def generateOneColumnUniform(spark: SparkSession, nbRow: Int) = {
    val dfIdx = spark.sqlContext.range(0,nbRow)
    val randomValues = dfIdx.select("id").withColumn("uniform", rand(42) * 100)
    randomValues.select("uniform")
  }

  def generateOneColumnGaussian(spark: SparkSession, nbRow: Int) = {
    val dfIdx = spark.sqlContext.range(0,nbRow)
    val randomValues = dfIdx.select("id")
                          .withColumn("uniform", randn(42) * 100)
    randomValues.select("gaussian")
  }


  def generate1ColTable(spark: SparkSession, nbRow: Int) = {
    def randomDouble1to100 = scala.util.Random.nextDouble * 100
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

  def generateBeta1ColTable(spark: SparkSession, nbRow: Int, alpha: Double, beta: Double) = {
    def randomBeta = Beta.distribution(alpha, beta).draw() * 100
    import spark.implicits._
    val df = spark.sparkContext.parallelize((Seq.fill(nbRow){(randomBeta)})).toDF("col1")
    df
  }

  // def save(df: DataFrame, path: String) = {
  //   df.write.parquet(path)
  // }
}
