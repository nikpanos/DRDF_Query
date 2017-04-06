package gr.unipi.datacron.queries

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts
import gr.unipi.datacron.plans.logical.starSTRange._

case class StarSptRangeQuery(config: Config) extends BaseQuery(config) {
  override def execute(): Unit = {
    val plan = config.getStringList(Consts.qfpLogicalPlans).get(0) match {
      case Consts.spatialFirstStarSptRangeLPlan => Some(StarSpatialFirst(config))
      case Consts.rdfFirstStarSptRangeLPlan => Some(StarRdfFirst(config))
      case Consts.spatialFirstJoinStarSptRangeLPlan => Some(StarSpatialFirstJoinST(config))
      case _ => None
    }

    if (plan.isDefined) {
      val result = plan.get.executePlan.cache
      result.show
      println(result.count)
    }
  }
}
