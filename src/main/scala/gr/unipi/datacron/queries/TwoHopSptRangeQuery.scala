package gr.unipi.datacron.queries

import gr.unipi.datacron.common.{AppConfig, Consts}
import gr.unipi.datacron.plans.logical.joinSTRange.JoinSpatialFirst

case class TwoHopSptRangeQuery() extends BaseQuery() {
  private[queries] override def doExecute(): Unit = {
    val plan = AppConfig.getStringList(Consts.qfpLogicalPlans).get(0) match {
      case Consts.spatialFirstJoinSptRangeLPlan => Some(JoinSpatialFirst())
      case _ => None
    }

    if (plan.isDefined) {
      val result = plan.get.executePlan.cache
      result.show
      println(result.count)
    }
  }
}
