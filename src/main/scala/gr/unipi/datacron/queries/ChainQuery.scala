package gr.unipi.datacron.queries

import gr.unipi.datacron.common.{AppConfig, Consts}
import gr.unipi.datacron.plans.logical.chainSTRange.ChainSTRange
import gr.unipi.datacron.plans.logical.starSTRange._

case class ChainQuery() extends BaseQuery() {
  private[queries] override def doExecute(): Unit = {
    val plan = AppConfig.getStringList(Consts.qfpLogicalPlans).get(0) match {
      case Consts.propertiesChainQueryPlan => Some(ChainSTRange())
      case _ => None
    }

    if (plan.isDefined) {
      println("Starting global time counter")
      val startTime = System.currentTimeMillis
      val result = plan.get.executePlan.cache
      println("Result count: " + result.count)
      result.show(100, truncate = false)
      //result.explain(true)
      val endTime = System.currentTimeMillis
      //result.explain()
      println("Global execution time (ms): " + (endTime - startTime))
    }
    else {
      println("Not a valid logical plan")
    }
  }
}