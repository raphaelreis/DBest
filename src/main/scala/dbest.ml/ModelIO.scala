package dbest.ml

import java.io._
import org.apache.log4j.{Level, Logger}
import breeze.numerics.log
import org.apache.spark.ml.PipelineModel
import java.nio.file.Files
import java.nio.file.Paths
import tools.makeFileName._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.CrossValidatorModel


class ModelIO(dir: String, df: DataFrame, x: Array[String], y: String, trainingFrac: Double){
    
    val logger = Logger.getLogger(this.getClass().getName())

    def exists(model: DBEstRegressor) = {
        val folderPath = makeFileName(dir, df, model, x, y, trainingFrac)
        Files.exists(Paths.get(folderPath))
    }

    def writeModel(model: DBEstRegressor) {
        val fileName = makeFileName(dir, df, model, x, y, trainingFrac)
        model.save(fileName)
    }
    
    def readModel(model: DBEstRegressor) = {
        val fileName = makeFileName(dir, df, model, x, y, trainingFrac)
        model.read(fileName)
        model
    }
}