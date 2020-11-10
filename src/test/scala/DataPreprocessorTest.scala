import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import Tools.hasColumn

class DataPreprocessorTest extends FunSuite {
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


    

    test("DataPreprocessor test regressionFilter") {
        
        val features = Array("col1")
        val label = "col3"

        val dp = new DataPreprocessor.DataPreprocessor(df, features, label)
        dp.regressionFilter()
        val preprocessedData: org.apache.spark.sql.DataFrame = dp.getPreprocessedDF()

        preprocessedData.show()

        assert(hasColumn(preprocessedData, "features"))
        assert(hasColumn(preprocessedData, "label"))
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