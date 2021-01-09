package traits

import scala.collection.mutable.Map

trait Analyser {
  var dfSize: Long
  var dfMins: Map[String,Double]
  var dfMaxs: Map[String,Double]
}