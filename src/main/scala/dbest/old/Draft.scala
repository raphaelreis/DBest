package dbest

import java.io.{File => JFile, PrintWriter}
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import org.apache.log4j.Logger
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValueFactory
import settings.Settings
import scala.collection.mutable.Map
import dbest.ml.ModelWrapper
import better.files._
import client._

object Draft {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass().getName())
    val confFileName = "conf/application.conf"
    val newDensityPathConfig = ConfigFactory.parseString("app.densitiesPath=${basedir}models/overhead_models/densities/")
    val newRegressionPathConfig = ConfigFactory.parseString("app.regressionPath=${basedir}models/overhead_models/regressions/")
    val oldConf = ConfigFactory.parseFile(new JFile(confFileName))
    val conf = newDensityPathConfig.withFallback(newRegressionPathConfig).withFallback(oldConf).resolve()             
    val settings = new Settings(conf)


    settings.printVars
  }
}
