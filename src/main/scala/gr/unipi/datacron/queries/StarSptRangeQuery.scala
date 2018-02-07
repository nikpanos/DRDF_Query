package gr.unipi.datacron.queries

import gr.unipi.datacron.common._
import gr.unipi.datacron.plans.logical.starSTRange._

case class StarSptRangeQuery() extends BaseQuery() {
  private[queries] override def doExecute(): Unit = {
    val plan = AppConfig.getStringList(Consts.qfpLogicalPlans).get(0) match {
      case Consts.spatialFirstStarSptRangeLPlan => Some(StarSpatialFirst())
      case Consts.rdfFirstStarSptRangeLPlan => Some(StarRdfFirst())
      case Consts.spatialFirstJoinStarSptRangeLPlan => Some(StarSpatialFirstJoinST())
      case Consts.rdfFirstBestStarSptRangeLPlan => Some(StarRdfFirstBest())
      case Consts.propertiesStarSptRangeLPlan => Some(StarProperties())
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

      //result.explain(true)
    }
    else {
      println("Not a valid logical plan")
    }
  }
}
