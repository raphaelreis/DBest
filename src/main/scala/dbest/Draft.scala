package dbest

import java.io._
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import scala.collection.mutable.Map

object Draft {
  def main(args: Array[String]): Unit = {
    
    val probVal = Double.MaxValue + 1.0
    val map = Map("count"->0.9999999999999999, "avg" -> probVal, "sum" -> 0.9999999999999999)
    val str = Json.stringify(Json.toJson(map))
    val errWriteName = "test.json"
    new PrintWriter(errWriteName) { write(str); close() }

  }
}