
import org.apache.log4j.Logger
import scala.collection.mutable.Map
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{SparkSession, functions=>F}

import dbest.ml.GroupByModelWrapper


class QueryEngineTest extends AnyFunSuite {
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
            Seq.fill(100){(randomIntTo100,randomDouble1to100, randomDouble1to100,randomDouble1to100)}
        )
        .toDF("col0", "col1", "col2", "col3")

    df.createOrReplaceTempView("df")
    val groupByDf = spark.sql("select col0 % 10 as groupId, col1, col2, col3 from df")  

    test("QueryEngine test groupByApproxCount for two features feature method") {
        val groupColumn = "groupId"
        val features = Array("col1", "col2")
        val label = "col3"

        //Could be compute in data processor and must be tracked
        val numberDataPoints = groupByDf.count().toInt
        val groupValuesCount = groupByDf.select(groupColumn).groupBy(groupColumn).count.rdd.map(r => (r.get(0), r.getLong(1).toInt)).collect()

        val dp = new dbest.dataprocessor.DataProcessor(groupByDf, features, label)
        dp.processForRegression()
        dp.processForGroupByDensity(groupColumn)

        val gmw: GroupByModelWrapper = new GroupByModelWrapper()
        gmw.fit(dp, groupColumn)

        var dfMins = Map[String, Double]()
        var dfMaxs = Map[String, Double]()
        val groupByDfCount = df.count()
        for (col <- groupByDf.columns) {
        val minMax = groupByDf.agg(F.min(col), F.max(col)).head.toSeq
        val (minimum: Double, maximum: Double) = (minMax(0).toString().toDouble, minMax(1).toString().toDouble)
            dfMins += col -> minimum
            dfMaxs += col -> maximum
        }

        /**
          * @BUG: the groupBymodelWrapper can feat only with several features
          */
        logger.info("getKdeModels: " + gmw.getKdeModels().mkString)

        val qe: engine.QueryEngine = new engine.QueryEngine(spark, groupByDfCount, dfMins, dfMaxs)
        val (countPerGroupValue, computeTime) = qe.groupByApproxCount(
                                              gmw, 
                                              groupValuesCount,
                                              groupColumn,
                                              features,
                                              0.0,
                                              100.0,
                                              0.001
                                            )
        logger.info(s"Is estimation empty ? ${countPerGroupValue.isEmpty}")
        logger.info("GroupBy count estimation: ")
        logger.info(features(0) + countPerGroupValue(features(0)).mkString)
        logger.info(features(1) + countPerGroupValue(features(1)).mkString)       
        logger.info("GroupBy count real value: ")
        logger.info(groupValuesCount.mkString)
    }
}