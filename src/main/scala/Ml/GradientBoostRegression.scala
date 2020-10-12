package Ml

import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel}
import org.apache.spark.sql._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexerModel
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.PipelineModel
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.ml.feature.FeatureHasher

class GradientBoostRegressor(spark: SparkSession) {
    // var regressor: PipelineModel = _
    // var assemblerIndependant: VectorAssembler = _
    // val schema: StructType = DataTypes.createStructType(
    //     Array[StructField](DataTypes.createStructField("feature", DataTypes.DoubleType, false))
    // )

    var model: GBTRegressionModel = _

    def fit(x: RDD[Double], y: RDD[Double]) = {
        // reg.fit(x, y)
        throw ???
    }

    def fit(df: DataFrame, y: String): Unit = {
        val predictiveColumns = df.columns.filter(! _.contains(y))

        val hasher = new FeatureHasher()
            .setInputCols(predictiveColumns: _*)
            .setOutputCol("features")

        val featurized = hasher.transform(df).select(y, "features").cache()

        val featureIndexer = new VectorIndexer()
            .setInputCol("features")
            .setOutputCol("indexedFeatures")
            .setMaxCategories(4)
            .fit(featurized)

        // val Array(trainingData, testData) = featurized.randomSplit(Array(0.7, 0.3))

        val reg = new GBTRegressor()
            .setLabelCol(y)
            .setFeaturesCol("indexedFeatures")
            .setMaxIter(10)

        model = reg.fit(featurized)
    }

    // def fit(table: String, independant: String, dependant: String): Unit = {
    //     var data: Dataset[Row] = spark.sql(s"SELECT $independant, $dependant FROM $table")
    //         .filter(s"$independant IS NOT NULL")
    //         .filter(s"$dependant IS NOT NULL")

    //     //rename the dependent column to be "label"
    //     data = data.toDF(independant,"label").as("data")

    //     //convert the independet column to be type of VectorUDT
    //     val columnIndependant: Array[String] = Array(independant)
    //     var assemblerIndependant = new VectorAssembler()
    //         .setInputCols(columnIndependant)
    //         .setOutputCol("features")

    //     val training: Dataset[Row] = assemblerIndependant.transform(data)

    //     // Split the data into training and test sets (30% held out for testing).
    //     val splits: Array[Dataset[Row]] = training.randomSplit(Array[Double](0.7, 0.3))
    //     val trainingData: Dataset[Row] = splits(0)
    //     val testData: Dataset[Row] = splits(1)

    //     // Automatically identify categorical features, and index them.
    //     // Set maxCategories so features with > 4 distinct values are treated as continuous.
    //     val featureIndexer: VectorIndexerModel = new VectorIndexer()
    //         .setInputCol("features")
    //         .setOutputCol("indexedFeatures")
    //         .setMaxCategories(4)
    //         .fit(trainingData)

    //     // Train a GBT model.
    //     val gbt: GBTRegressor = new GBTRegressor()
    //         .setLabelCol("label")
    //         .setFeaturesCol("indexedFeatures")
    //         .setMaxIter(10)

    //     // Chain indexer and GBT in a Pipeline.
    //     val pipeline: Pipeline = new Pipeline().setStages(Array[PipelineStage](featureIndexer, gbt))

    //     // Train model. This also runs the indexer.
    //     var regressor = pipeline.fit(trainingData)
    // }
    
    // def predict(point: Array[Double]): Double = {
    //     val sc = spark.sparkContext
    //     val doubleAsList = Array[Double](point(0))

    //     val rowRDD: RDD[Row] = sc.parallelize(doubleAsList).map((row: Double) => Row(row))
    //     val pt: Dataset[Row] = spark.sqlContext.createDataFrame(rowRDD, schema).toDF()

    //     val training: Dataset[Row] = assemblerIndependant.transform(pt)
    //     val featureIndexer: VectorIndexerModel = new VectorIndexer()
    //         .setInputCol("features")
    //         .setOutputCol("indexedFeatures")
    //         .setMaxCategories(4)
    //         .fit(training)

    //     val predictions: Dataset[Row] = regressor.transform(training)

    //     val index: Int = predictions.schema().getFieldIndex("predictions").get().asInstanceOf[Int]
    //     val result: Double = predictions.first().getDouble(index)

    //     println(result)
    //     result
    // }
}