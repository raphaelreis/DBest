package sampler

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Sampler {
    //Uniform sampling
    def uniformSampling(df: DataFrame, fraction: Double): DataFrame = df.sample(fraction) 

    //Stratified sampling
    def stratifiedSampling(spark: SparkSession, df: DataFrame, groupByColumn: String, features: Array[String],
        label: String,  fraction: Map[Int, Double]) = {
            import spark.implicits._
            df.select(groupByColumn, features(0), label)
                .rdd
                .map(r=> (r.getInt(0), (r.getDouble(1), r.getDouble(2))))
                .sampleByKey(false, fraction)
                .map{
                    case (g, (f, l)) => (g, f, l)
                }
                .toDF(groupByColumn, features(0), label)           
    }
    //Reservoir sampling
}