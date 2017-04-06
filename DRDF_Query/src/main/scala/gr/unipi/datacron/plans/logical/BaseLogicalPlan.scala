package gr.unipi.datacron.plans.logical

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

abstract private[logical] class BaseLogicalPlan(config: Config) {
  private[logical] val queryName: String = config.getString(Consts.qfpQueryName)
  private[logical] val nTotalBits: Int = config.getInt(Consts.qfpTotalBits)
  private[logical] val nSpatialBits: Int = config.getInt(Consts.qfpSpatialBits)
  private[logical] val nIDsBits: Int = config.getInt(Consts.qfpIDsBits)
  DataStore.init(config)
  PhysicalPlanner.init(config)
  
  def executePlan: DataFrame = {
    println("Query name: " + config.getString(Consts.qfpQueryName))
    println("Query type: " + config.getString(Consts.qfpQueryType))
    doExecutePlan(DataStore.triplesData, DataStore.dictionaryData)
  }

  private[logical] def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame
}