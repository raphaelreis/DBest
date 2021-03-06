package dbest.ml

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel

import traits.DBEstModel

abstract class DBEstRegressor extends DBEstModel {
  var cvModel: CrossValidatorModel = _
  def fit(traingSet: DataFrame): PipelineModel
  def crossValidate(traingSet: DataFrame, numFolds: Int, numWorkers: Int): CrossValidatorModel
  def save(path: String) {
    cvModel.save(path)
  }
  def read(path: String) {
    cvModel = CrossValidatorModel.load(path)
  }
  def transform(df: DataFrame): DataFrame = {
    cvModel.transform(df)
  }
}