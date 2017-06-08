package gr.unipi.datacron.plans.logical

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

abstract private[logical] class BaseLogicalPlan() {
  private[logical] val queryName: String = AppConfig.getString(Consts.qfpQueryName)
  private[logical] val nTotalBits: Int = AppConfig.getInt(Consts.qfpTotalBits)
  private[logical] val nSpatialBits: Int = AppConfig.getInt(Consts.qfpSpatialBits)
  private[logical] val nIDsBits: Int = AppConfig.getInt(Consts.qfpIDsBits)

  private[logical] val encoder = new SimpleEncoder()
  
  def executePlan: DataFrame = {
    println("Query name: " + AppConfig.getString(Consts.qfpQueryName))
    println("Query type: " + AppConfig.getString(Consts.qfpQueryType))
    doExecutePlan(DataStore.triplesData, DataStore.dictionaryData)
  }

  private[logical] def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame

  private[logical] val constraints = SpatioTemporalRange(
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), AppConfig.getLong(qfpTimeLower)),
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), AppConfig.getLong(qfpTimeUpper)))
}