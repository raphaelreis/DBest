import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import DBestClient._
import Ml._
import java.nio.file.{Paths, Files}
import java.io.File

class ModelIOTest extends FunSuite {
    val logger = Logger.getLogger(this.getClass().getName())

    val root = ""
    val fileName = "data/store_sales_sample.dat"

    var client: DBestClient = new DBestClient(root + fileName)
    val df_client = client.df
   
    test("ModelIO.writeModel for LinearRegressor") {
        
        val x = Array("_c13")
        val y = "_c20"
        
        val lr = new LinearRegressor()
        val model = lr.fit(df_client, x, y)
        val mio = new ModelIO(lr, x, y)
        mio.writeModel()

        val filePath = "/Users/Raphael/CS/github.com/DBest/src/main/resources/models/linreg/_c13_c20"
        assert(Files.exists(Paths.get(filePath)))
    }

    test("ModelIO.writeModel for SparkKernelDensity") {
        
        val x = Array("_c13")
        val y = "_c20"
        
        val kd = new SparkKernelDensity()
        val model = kd.fit(df_client, x)
        val mio = new ModelIO(model, x, y)
        mio.writeModel()

        val filePath = "/Users/Raphael/CS/github.com/DBest/src/main/resources/models/kd/_c13"
        val bool = new File(filePath).isFile
        logger.info(s"runtime boolean value: $bool")
        assert(bool)
    }
}