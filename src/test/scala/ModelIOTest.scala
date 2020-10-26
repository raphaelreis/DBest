import org.scalatest.FunSuite
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import DBestClient._
import Ml._
import DataLoader._
import java.nio.file.{Paths, Files}
import java.io.File
import org.apache.spark.ml.regression.LinearRegressionModel


class ModelIOTest extends FunSuite {
    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession
        .builder
        .master("local")
        .appName("ModelIO test")
        .getOrCreate()

    val dload = new DataLoader(spark)

    val root = ""
    val fileName = "data/store_sales_sample.dat"
    val table = root + fileName
    var df: DataFrame = _

    if (Files.exists(Paths.get(table))) {
        df = dload.loadTable(table)
    } else {
        throw new Exception("Table does not exist.")
    }
   
    // test("ModelIO.writeModel for LinearRegressor") {
        
    //     val x = Array("_c13")
    //     val y = "_c20"
        
    //     val lr = new LinearRegressor()
    //     val model = lr.fit(df, x, y)
    //     val mio = new ModelIO(lr, x, y)
    //     mio.writeModel()

    //     val filePath = "/Users/Raphael/CS/github.com/DBest/src/main/resources/models/linreg/_c13_c20"
    //     assert(Files.exists(Paths.get(filePath)))
    // }

    // test("ModelIO.writeModel for SparkKernelDensity") {
        
    //     val x = Array("_c13")
    //     val y = "_c20"
        
    //     val kd = new SparkKernelDensity()
    //     val model = kd.fit(df, x)
    //     val mio = new ModelIO(model, x, y)
    //     mio.writeModel()

    //     val filePath = "/Users/Raphael/CS/github.com/DBest/src/main/resources/models/kd/_c13"
    //     val bool = new File(filePath).isFile
    //     logger.info(s"runtime boolean value: $bool")
    //     assert(bool)
    // }

    // test("ModelIO.readModel for Kernel Density with random df") {
    //     def randomDouble1to100 = scala.util.Random.nextDouble * 100

    //     import spark.implicits._
    //     val dfRdm = spark.sparkContext
    //         .parallelize(
    //             Seq.fill(100){(randomDouble1to100,randomDouble1to100,randomDouble1to100)}
    //         )
    //         .toDF("col1", "col2", "col3")
        
    //     val x = Array("col1")
    //     val y = "col3"

    //     //Write KD model
    //     val kd = new SparkKernelDensity()
    //     val model = kd.fit(dfRdm, x)

    //     val mio = new ModelIO(x, y)
    //     mio.writeModel(model)

    //     //Read KD Model
    //     val modelRead = mio.readModel(model).asInstanceOf[SparkKernelDensity]

        
    //     // logger.info(model)
    //     // logger.info(modelRead)

    //     modelRead.kd
    //     spark.sparkContext.parallelize()
    //     val toPredict = Array.fill(10){randomDouble1to100}
    //     val modPred = model.predict(toPredict)
    //     val modReadPred = modelRead.predict(toPredict)

    //     assert(modPred.deep == modReadPred.deep)
    // }   


    // test("ModelIO.readModel for LinearRegression with random df") {
    //     def randomInt1to100 = scala.util.Random.nextInt(100)+1

    //     import spark.implicits._
    //     val dfRdm = spark.sparkContext
    //         .parallelize(
    //             Seq.fill(100){(randomInt1to100,randomInt1to100,randomInt1to100)}
    //         )
    //         .toDF("col1", "col2", "col3")
        
    //     val x = Array("col1")
    //     val y = "col3"

    //     //Write KD model
    //     val reg = new LinearRegressor()
    //     val regFit = reg.fit(dfRdm, x, y)

    //     val mio = new ModelIO(x, y)
    //     mio.writeModel(regFit)

    //     //Read KD Model
    //     val modelRead = mio.readModel(regFit)
        
    //     assert(regFit.equals(modelRead))
    // }   

    
}