package Ml

import java.io._
import org.apache.log4j.{Level, Logger}
import breeze.numerics.log
import org.apache.spark.ml.PipelineModel
import java.nio.file.Files
import java.nio.file.Paths
import Tools._


class ModelIO(dir: String, x: Array[String], y: String){
    
    val logger = Logger.getLogger(this.getClass().getName())

    def exists(model: DBestModel) = {
        val folderPath = makeFileName(dir, model, x, y)
        Files.exists(Paths.get(folderPath))
    }

    def writeModel(model: DBestModel) = model match {
        case model: LinearRegressor => {
            val fileName = makeFileName(dir, model, x, y)
            model.save(fileName)
        }
        case model: SparkKernelDensity => throw new Exception("Cannot write SparkKernelDensity")
    }
    
    def readModel(model: DBestModel) = model match {
        case model: LinearRegressor => {
            val fileName = makeFileName(dir, model, x, y)
            val pipemodel = PipelineModel.load(fileName)
            model.setModel(pipemodel)
            model
        }
        case model: SparkKernelDensity => throw new Exception("Cannot read SparkKernelDensity")
    }
}