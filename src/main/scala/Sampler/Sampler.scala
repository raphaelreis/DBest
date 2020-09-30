package Sampler

import org.apache.spark.sql.DataFrame

class Sampler(df: DataFrame) {
    //Uniform sampling
    def uniformSampling(fraction: Double): DataFrame = df.sample(fraction) 

    //Stratified sampling
    // def stratSampling[T](df: org.apache.spark.sql.DataFrame, col: org.apache.spark.sql.Column, frac: java.util.Map[T,java.lang.Double], seed: Long)
        // = df.stat.sampleBy(col, frac, seed)

    //Reservoir sampling
}