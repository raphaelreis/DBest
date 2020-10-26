package Ml

import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.ml.feature.FeatureHasher
import shapeless.Data
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel

class LinearRegressor extends DBestModel {
    
    val name = "linreg"
    var model: PipelineModel = _

    def getLinearRegressionModel() = model.stages(1).asInstanceOf[LinearRegressionModel]
    
    def fit(df: DataFrame, x: Array[String], y: String): LinearRegressor = {

        val assembler = new VectorAssembler()
            .setInputCols(x)
            .setOutputCol("features")
        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol(y)
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
        val pipeline = new Pipeline()
            .setStages(Array(assembler, lr))

        model = pipeline.fit(df)
        this
    }

    def setModel(mod: PipelineModel) = {
        model = mod
    }

    def save(fileName: String) = {
        model.write.overwrite().save(fileName)
    }
}