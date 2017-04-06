package gr.unipi.datacron.queries

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.operators.Executor
import org.apache.spark.sql.DataFrame

abstract class BaseQuery(config: Config) {
  private[queries] val queryName: String = config.getString(Consts.qfpQueryName)
  private[queries] val nTotalBits: Int = config.getInt(Consts.qfpTotalBits)
  private[queries] val nSpatialBits: Int = config.getInt(Consts.qfpSpatialBits)
  private[queries] val nIDsBits: Int = config.getInt(Consts.qfpIDsBits)
  DataStore.init(config)
  Executor.init(config)
  
  def executeQuery: DataFrame = {
    println("Query name: " + config.getString(Consts.qfpQueryName))
    println("Query type: " + config.getString(Consts.qfpQueryType))
    doExecuteQuery(DataStore.triplesData, DataStore.dictionaryData)
  }

  private[queries] def doExecuteQuery(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame
}