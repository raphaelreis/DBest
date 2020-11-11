package DataProcessor

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.IntegerType

class DataProcessor(dataFrame: DataFrame, features: Array[String], label: String) {

    private var df: DataFrame = dataFrame
    private val mapRDD = Map.empty[String, RDD[Double]]
    private val mapRDDforGroupBy = Map.empty[String, RDD[(AnyVal, Double)]]
    var assembled: Boolean = false

    def assemble(): Unit = {
        assembled = true
        val assembler = new VectorAssembler()
            .setInputCols(features)
            .setOutputCol("features")
        df = assembler.transform(df)
    }

    /**
      * Preprocess the dataframe for a data regression modeling
      *
      * @return the DataPreprocessor object
      */
    def processForRegression(): DataProcessor = {
        assemble()
        df = df.withColumn("label", df.col(label))
        this
    }

    def processForDensity(): DataProcessor = {
        val selectedDF = dataFrame.select(features.head, features.tail: _*)
        for (i <- 0 to features.length-1){
            mapRDD += features(i) -> selectedDF.rdd.map(r => r.getDouble(i))
        }
        this
    }

    def processForGroupByDensity(groupColumn: String): DataProcessor = {
        val selectedDF = dataFrame.select(groupColumn, features: _*)

        val groupByType = selectedDF.select(groupColumn).schema.fields(0).dataType match {
            case DoubleType => {
                for (i <- 1 to features.length-1){
                    mapRDDforGroupBy += features(i) -> selectedDF.rdd.map(r => (r.getDouble(0), r.getDouble(i)))  
                }
            }
            case IntegerType => {
                for (i <- 1 to features.length-1){
                    mapRDDforGroupBy += features(i) -> selectedDF.rdd.map(r => (r.getInt(0), r.getDouble(i)))  
                }
            }
            case _ => throw new Exception("Not supported group by column type (Only Int and Double)")
        }
        this
    }

    def getPreprocessedDF(): DataFrame = df
    def getMapRDD(): Map[String, RDD[Double]] = mapRDD

}