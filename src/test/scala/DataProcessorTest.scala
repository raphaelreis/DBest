import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import Tools.hasColumn
import scala.collection.mutable
import org.apache.spark.rdd.RDD

class DataProcessorTest extends FunSuite {
    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("Data loader")
        .getOrCreate()

    def randomDouble1to100 = scala.util.Random.nextDouble * 100
    def randomIntTo100 = scala.util.Random.nextInt(100) + 1

    import spark.implicits._
    val df = spark.sparkContext
        .parallelize(
            Seq.fill(100){(randomIntTo100,randomDouble1to100,randomDouble1to100)}
        )
        .toDF("col1", "col2", "col3")
        
    // test("DataProcessor test processForRegression") {
        
    //     val features = Array("col1", "col2")
    //     val label = "col3"

    //     val dp = new DataProcessor.DataProcessor(df, features, label)
    //     dp.processForRegression()
    //     val preprocessedData: org.apache.spark.sql.DataFrame = dp.getPreprocessedDF()

    //     preprocessedData.show()

    //     assert(hasColumn(preprocessedData, "features"))
    //     assert(hasColumn(preprocessedData, "label"))
    // }

    // test("DataProcessor test processForDensity") {
        
    //     val features = Array("col2")
    //     val label = "col3"

    //     val dp = new DataProcessor.DataProcessor(df, features, label)
    //     dp.processForDensity()
    //     val mapRDD: mutable.Map[String, RDD[Double]] = dp.getMapRDD()

    //     features.map{
    //         col: String => {
    //             val column = df.select(col).rdd.map(_.getDouble(0)).collect()
    //             mapRDD.get(col) match {
    //                 case Some(value) => assert(column.deep == value.collect().deep)
    //                 case None => throw new Exception(s"The column $col is not in the mapRDD")
    //             }
    //         }
    //     }
    // }

    test("DataProcessor test processForGroupByDensities") {
        
    }
}

    // test("DataPreprocessor test full") {
        
    //     val features = Array("col1", "col2")
    //     val label = "col3"

    //     val dp = new DataPreprocessor.DataPreprocessor(df, features, label)
    //     dp.regressionFilter().kdeFilter()
    //     val preprocessedData: org.apache.spark.sql.DataFrame = dp.preprocess()

    //     preprocessedData.show()
        

    //     df.show()

    //     assert(true)
    // }
// }