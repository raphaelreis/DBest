package Ml

import java.io._
import org.apache.log4j.{Level, Logger}
import breeze.numerics.log


class ModelIO(model: DBestModel, x: Array[String], y: String){
    
    val logger = Logger.getLogger(this.getClass().getName())

    /**
      * Change for relative file path
      */
    var fileName = "/Users/Raphael/CS/github.com/DBest/src/main/resources/models/" + model.name + "/" + x.mkString("_")

    def writeModel() = model match {
        case model: LinearRegressor => {
            fileName = fileName + y
            model.save(fileName)
        }
        case model: SparkKernelDensity => {
            logger.info("Start serialization kernel density")
            logger.info("fileName: " + fileName)
            val oos = new ObjectOutputStream(new FileOutputStream(fileName))
            oos.writeObject(model)
            oos.close
        }
    }
    

}