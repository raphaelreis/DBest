package settings

import com.typesafe.config.Config

class Settings(config: Config) {
    // non-lazy fields, we want all exceptions at construct time
    val hdfsAvailable = config.getBoolean("app.hdfsAvailable")
    println(hdfsAvailable)
    val resultsFolder = config.getString("app.resultsFolder")
    val dpath = config.getString("app.densitiesPath")
    val rpath = config.getString("app.regressionPath")
}