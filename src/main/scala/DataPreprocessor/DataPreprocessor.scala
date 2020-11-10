package DataPreprocessor

import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline

class DataPreprocessor(dataFrame: DataFrame, features: Array[String], label: String) {

    var pipeLine: Pipeline = _
    var df: DataFrame = dataFrame
    var assembled: Boolean = false


    def assemble(): Unit = {
        assembled = true
        val assembler = new VectorAssembler()
            .setInputCols(features)
            .setOutputCol("features")
        df = assembler.transform(df)
    }

    /**
      * Preprocess the dataframe df for a data regression modeling
      *
      * @return the DataPreprocessor object
      */
    def regressionFilter(): DataPreprocessor = {
        assemble()
        df = df.withColumn("label", df.col(label))
        this
    }

    // /**
    //   * Preprocess the dataframe df for data kernel density estimation 
    //   *
    //   * @return
    //   */
    // def kdeFilter(isFinal: Boolean): DataPreprocessor = {
    //     if (isFinal) {
    //         if(assembled) {
    //             df = df.select(label, "features" +: features: _*)
    //         } else {
    //             df = df.select(label, features: _*)
    //         }
    //     }
    //     this
    // }

    def getPreprocessedDF(): DataFrame = df

}