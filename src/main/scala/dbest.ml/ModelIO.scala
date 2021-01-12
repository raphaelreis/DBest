package dbest.ml

import java.io._
import org.apache.log4j.{Level, Logger}
import breeze.numerics.log
import org.apache.spark.dbest.ml.PipelineModel
import java.nio.file.Files
import java.nio.file.Paths
import tools.makeFileName._
import org.apache.spark.sql.DataFrame

class ModelIO(dir: String, df: DataFrame, x: Array[String], y: String, trainingFrac: Double){
    
    val logger = Logger.getLogger(this.getClass().getName())

    def exists(model: DBEstRegressor) = {
        val folderPath = makeFileName(dir, df, model, x, y, trainingFrac)
        Files.exists(Paths.get(folderPath))
    }

    def writeModel(model: DBEstRegressor) = model match {
        case model: LinearRegressor => {
            val fileName = makeFileName(dir, df, model, x, y, trainingFrac)
            model.save(fileName)
        }
    }
    
    def readModel(model: DBEstRegressor) = model match {
        case model: LinearRegressor => {
            val fileName = makeFileName(dir, df, model, x, y, trainingFrac)
            val pipemodel = PipelineModel.load(fileName)
            model.setModel(pipemodel)
            model
        }
    }
}