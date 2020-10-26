package Tools

import Ml.DBestModel

object makeFileName extends ((DBestModel, Array[String], String) => String) {
    def apply(model: DBestModel, x: Array[String], y: String): String = {
        val root = "/Users/Raphael/CS/github.com/DBest/src/main/resources/models/"
        root + model.name + "/" + x.mkString("_") + y
    }
}