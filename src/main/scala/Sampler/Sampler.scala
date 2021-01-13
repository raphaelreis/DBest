package sampler

import org.apache.spark.sql.DataFrame

object Sampler {
    //Uniform sampling
    def uniformSampling(df: DataFrame, fraction: Double): DataFrame = df.sample(fraction) 

    //Stratified sampling
    // def stratSampling[T](df: org.apache.spark.sql.DataFrame, col: org.apache.spark.sql.Column, frac: java.util.Map[T,java.lang.Double], seed: Long)
        // = df.stat.sampleBy(col, frac, seed)

    //Reservoir sampling
}