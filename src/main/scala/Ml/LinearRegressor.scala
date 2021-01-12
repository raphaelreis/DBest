package ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import tools.hasColumn
import org.apache.log4j.{Level, Logger}
import traits.DBEstModel

class LinearRegressor extends DBEstModel {
    private val logger = Logger.getLogger(this.getClass().getName())
    val name = "linear_regressor"
    var model: PipelineModel = _

    def getLinearRegressionModel() = model.stages(1).asInstanceOf[LinearRegressionModel]
    def getLinearRegressionModel(i: Int) = model.stages(i).asInstanceOf[LinearRegressionModel]

    def transform(df: DataFrame) = {
        model.transform(df)
    }
    
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

    def fit(df: DataFrame): LinearRegressor = {
        
        if (!hasColumn(df, "features")) {
            throw  new Exception("Cannot fit linear regression since dataframe has no features column")
        }

        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            
        val pipeline = new Pipeline()
            .setStages(Array(lr))

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