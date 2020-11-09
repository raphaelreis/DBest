import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}


class GroupedByUDAFTest extends FunSuite {
    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("Data loader")
        .getOrCreate()

    def randomDouble1to100 = scala.util.Random.nextDouble * 100

    import spark.implicits._
    val df = spark.sparkContext
        .parallelize(
            Seq.fill(100){(randomDouble1to100,randomDouble1to100,randomDouble1to100)}
        )
        .toDF("col1", "col2", "col3")

    
    test("GroupedByUDAF ") {
        // df.show()

        // smile.regression.LASSO.fit()
        import org.apache.spark.sql.functions._

        val ids = spark.range(1, 20)
        ids.createOrReplaceTempView("ids")
        val df = spark.sql("select id, id % 3 as group_id from ids")
        df.createOrReplaceTempView("simple")
        df.show()


        assert(true)

    }
}