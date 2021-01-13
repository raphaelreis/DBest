import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import dbest.ml.GroupByModelWrapper
import dbest.ml.ModelWrapper
import org.apache.spark.sql.functions.{rand, randn}
import dbest.dataprocessor.dbest.dataprocessor
import settings.Settings
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.sql.{functions=>F}
import scala.collection.mutable.Map

class ModelWrapperTest extends AnyFunSuite {
    val logger = Logger.getLogger(this.getClass().getName())

    val spark: SparkSession = SparkSession.builder
        .master("local")
        .appName("Data loader")
        .getOrCreate() 

    val confFileName = "conf/application.conf"
    val conf = ConfigFactory.parseFile(new File(confFileName)).resolve()
    val settings = new Settings(conf)

    test("ModelWrapper test saveDensities method") {
        val idx = spark.sqlContext.range(0, 100)
        val randomDf = idx.select("id").withColumn("uniform", rand(42))

        val features = Array("uniform")

        var dfMins = Map[String, Double]()
        var dfMaxs = Map[String, Double]()
        val randomDfCount = randomDf.count()
        for (col <- randomDf.columns) {
        val minMax = randomDf.agg(F.min(col), F.max(col)).head.toSeq
        val (minimum: Double, maximum: Double) = (minMax(0).toString().toDouble, minMax(1).toString().toDouble)
            dfMins += col -> minimum
            dfMaxs += col -> maximum
        }

        val mw = new ModelWrapper(settings, randomDfCount, dfMins, dfMaxs)
        mw.saveDensities(randomDf, features, settings.densitiyInterspacEvaluation, 1.0)
    }

    test("ModelWrapper test fitOrLoad method") {
        val idx = spark.sqlContext.range(0, 100)
        val randomDf = idx.select("id").withColumn("uniform1", rand(42)).withColumn("uniform2", rand(42)).withColumn("uniform3", rand(42))
        
        val features = Array("uniform1", "uniform2")
        val label = "uniform3"
        val dp = new dbest.dataprocessor(randomDf, features, label)
        dp.processForRegression()
        val df = dp.getPreprocessedDF()



        var dfMins = Map[String, Double]()
        var dfMaxs = Map[String, Double]()
        val randomDfCount = randomDf.count()
        for (col <- randomDf.columns) {
        val minMax = randomDf.agg(F.min(col), F.max(col)).head.toSeq
        val (minimum: Double, maximum: Double) = (minMax(0).toString().toDouble, minMax(1).toString().toDouble)
            dfMins += col -> minimum
            dfMaxs += col -> maximum
        }


        val mw = new ModelWrapper(settings, randomDfCount, dfMins, dfMaxs)
        mw.fitOrLoad("sum", df, features, label, 1.0)
    }

    test("GroupByModelWrapper test fit method") {
        def randomDouble1to100 = scala.util.Random.nextDouble * 100
        def randomIntTo100 = scala.util.Random.nextInt(100) + 1

        import spark.implicits._
        val df = spark.sparkContext
            .parallelize(
                Seq.fill(100){(randomIntTo100,randomDouble1to100, randomDouble1to100,randomDouble1to100)}
            )
            .toDF("col0", "col1", "col2", "col3")

        df.createOrReplaceTempView("df")
        val groupByDf = spark.sql("select col0 % 10 as groupId, col1, col2, col3 from df") 

        val groupColumn = "groupId"
        val features = Array("col1", "col2")
        val label = "col3"

        
        val dp = new dbest.dataprocessor.dbest.dataprocessor(groupByDf, features, label)
        dp.processForRegression()
        dp.processForGroupByDensity(groupColumn)

        val gmw: GroupByModelWrapper = new GroupByModelWrapper()
        gmw.fit(dp, groupColumn)
        
        logger.info(gmw.getKdeModels())
        assert(!gmw.getKdeModels().isEmpty)
        assert(!gmw.getRegModels().isEmpty)
    }
}