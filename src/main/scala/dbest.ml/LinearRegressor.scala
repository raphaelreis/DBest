package dbest.ml

import tools.hasColumn
import traits.DBEstModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder

class LinearRegressor extends DBEstRegressor with DBEstModel {
    private val logger = Logger.getLogger(this.getClass().getName())
    val name = "linear_regressor"

    def fit(traingSet: DataFrame, x: Array[String], y: String) = {
        val assembler = new VectorAssembler()
            .setInputCols(x)
            .setOutputCol("features")

        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol(y)
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
        val estimator = new Pipeline()
            .setStages(Array(assembler, lr))

        estimator.fit(traingSet)
    }

    def fit(traingSet: DataFrame): PipelineModel = {
        if (!hasColumn(traingSet, "features"))
            throw  new Exception("Cannot fit linear regression since dataframe has no features column")

        val lr = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol("label")
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            
        val estimator = new Pipeline()
            .setStages(Array(lr))

        estimator.fit(traingSet)
    }

    def crossValidate(trainingSet: DataFrame, numFolds: Int): CrossValidatorModel = {
        val lr  = new LinearRegression()
            .setFeaturesCol("features")
            .setLabelCol("label")

        val paramGrid = new ParamGridBuilder()
            .addGrid(lr.regParam, Array(0.1, 0.01))
            .addGrid(lr.elasticNetParam, Array(0.1, 0.5, 0.7, 0.9, 0.95, 0.99, 1.0))
            .build()

        val evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse")

        val cv = new CrossValidator()
        .setEstimator(lr)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(numFolds)

        cvModel = cv.fit(trainingSet)
        cvModel
    }    
}