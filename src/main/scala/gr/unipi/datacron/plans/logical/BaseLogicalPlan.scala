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

  private[logical] val encoder = SimpleEncoder()
  
  def executePlan: DataFrame = {
    println("Query type: " + AppConfig.getString(Consts.qfpQueryType))
    println("Logical plan: " + AppConfig.getStringList(Consts.qfpLogicalPlans).get(0))
    doExecutePlan()
  }

  private[logical] def doExecutePlan(): DataFrame

  private[logical] val constraints = SpatioTemporalRange(
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), AppConfig.getLong(qfpTimeLower)),
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), AppConfig.getLong(qfpTimeUpper)))
}