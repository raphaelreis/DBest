package settings

import com.typesafe.config.Config

class Settings(config: Config) {
    // non-lazy fields, we want all exceptions at construct time
    val baseDir = config.getString("basedir")
    val hdfsAvailable = config.getBoolean("app.hdfsAvailable")
    val resultsFolder = config.getString("app.resultsFolder")
    val dpath = config.getString("app.densitiesPath")
    val rpath = config.getString("app.regressionPath")
    val densitiyInterspacEvaluation = config.getDouble("app.densitiyInterspacEvaluation")
    val defaultKernelBandWidth = config.getDouble("app.defaultKernelBandWidth")
    val numberOfCores = config.getDouble("app.numberOfCores")
    val modelType = config.getString("app.modelType")
    val crossValNumFolds = if(config.getInt("app.crossValNumFolds") > 1) config.getInt("app.crossValNumFolds")
        else throw new RuntimeException("Wrong configuration crossValNumFolds value; must be > 1")
    val numWorkers = config.getInt("app.numWorkers")
    def printVars = this.getClass.getDeclaredFields.foreach{v=>v.setAccessible(true); println("Field: " + v.getName() + " => " + v.get(this))}

}