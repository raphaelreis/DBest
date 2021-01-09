package dbest

import org.apache.log4j.{Level, Logger}
import DBestClient.DBestClient

object FitModels {
  def main(args: Array[String]) = {
    val logger = Logger.getLogger(this.getClass().getName())

    val distribution = "normal"
    val resultsFolder = "results/"
    val path = s"data2/df10m_$distribution.parquet"
    val tableName = s"dataframe10m_$distribution"
    val client: DBestClient = new DBestClient
    client.loadHDFSTable(path, tableName)

    
  }
}