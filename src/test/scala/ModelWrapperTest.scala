import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import Ml.GroupByModelWrapper

class ModelWrapperTest extends FunSuite {
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

    df.createOrReplaceTempView("df")
    val groupByDf = spark.sql("select col1 % 10 as group_id, col2, col3 from df")  

    test("GroupByModelWrapper test fitReg method") {
        
        val features = Array("col2")
        val label = "col3"

        val dp = new DataProcessor.DataProcessor(groupByDf, features, label)
        dp.processForRegression()
        val processedDF = dp.getPreprocessedDF()

        val gmw: GroupByModelWrapper = new GroupByModelWrapper()
        gmw.fitRegs(processedDF, "group_id")
    }
}