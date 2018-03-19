package gr.unipi.datacron.queries

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.fixedPlans.starSTRange._

case class StarSptRangeQuery() extends BaseQuery() {

  protected[queries] override def getExecutionPlan(): Option[BaseLogicalPlan] =  AppConfig.getString(qfpLogicalPlans) match {
    case `spatialFirstStarSptRangeLPlan` => Some(StarSpatialFirst())
    case `rdfFirstStarSptRangeLPlan` => Some(StarRdfFirst())
    case `spatialFirstJoinStarSptRangeLPlan` => Some(StarSpatialFirstJoinST())
    case `rdfFirstBestStarSptRangeLPlan` => Some(StarRdfFirstBest())
    case `propertiesStarSptRangeLPlan` => Some(StarProperties())
  }
}
