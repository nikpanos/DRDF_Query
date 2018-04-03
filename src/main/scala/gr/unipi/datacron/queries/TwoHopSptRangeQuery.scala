package gr.unipi.datacron.queries

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.staticPlans.joinSTRange.JoinSpatialFirst
import gr.unipi.datacron.common.Consts._

case class TwoHopSptRangeQuery() extends BaseQuery() {

  protected[queries] override def getExecutionPlan(): Option[BaseLogicalPlan] = AppConfig.getString(qfpLogicalPlans) match {
    case `spatialFirstJoinSptRangeLPlan` => Some(JoinSpatialFirst())
  }
}
