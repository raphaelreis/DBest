package dbest.ml

import java.nio.file.Files
import java.nio.file.Paths
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.PipelineModel

import tools.makeFileName.makeFileName


class ModelIO(dir: String, df: DataFrame, x: Array[String], y: String, trainingFrac: Double){
    
    val logger = Logger.getLogger(this.getClass().getName())

    def exists(model: DBEstRegressor) = {
        val folderPath = makeFileName(dir, df, model, x, y, trainingFrac)
        Files.exists(Paths.get(folderPath))
    }

    def writeModel(model: DBEstRegressor) {
        val filePath = makeFileName(dir, df, model, x, y, trainingFrac)
        model.save("file:///" + filePath)
    }
    
    def readModel(model: DBEstRegressor) = {
        val filePath = makeFileName(dir, df, model, x, y, trainingFrac)
        model.read("file:///" + filePath)
        model
    }
}