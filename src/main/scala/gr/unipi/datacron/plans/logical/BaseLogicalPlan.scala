package gr.unipi.datacron.plans.logical

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

abstract class BaseLogicalPlan() {
  private[logical] val queryName: String = AppConfig.getString(Consts.qfpQueryName)

  private[logical] val encoder = SimpleEncoder()
  
  def executePlan: DataFrame = {
    //println("Query type: " + AppConfig.getString(Consts.qfpQueryType))
    //println("Logical plan: " + AppConfig.getString(Consts.qfpLogicalPlans))
    doExecutePlan()
  }

  private[logical] def doExecutePlan(): DataFrame

  def preparePlan(): Unit = { }
}