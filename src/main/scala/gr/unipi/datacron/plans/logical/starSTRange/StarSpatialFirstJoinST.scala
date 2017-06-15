package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

case class StarSpatialFirstJoinST() extends BaseStar() {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(dfTriples, constraints, encoder, Some("Filter by encoded spatiotemporal info")))

    val extendedTriples = SptRefinement.addSpatialAndTemporalColumns(filteredByIdInfo, filteredByIdInfo, dfDictionary)

    val qPredTrans = encodePredicate(dfDictionary, qPred)
    val qObjTrans = encodePredicate(dfDictionary, qObj)

    val filteredSPO = PhysicalPlanner.filterByPO(filterByPOParams(extendedTriples, qPredTrans, qObjTrans, Some("Filter by spo predicate"))).cache

    SptRefinement.refineResults(filteredSPO, dfTriples, dfDictionary, constraints)
  }
}
