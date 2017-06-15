package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

case class StarRdfFirst() extends BaseStar() {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val qPredTrans = encodePredicate(dfDictionary, qPred)
    val qObjTrans = encodePredicate(dfDictionary, qObj)

    val filteredSPO = PhysicalPlanner.filterByPO(filterByPOParams(dfTriples, qPredTrans, qObjTrans, Some("Filter by spo predicate")))
    
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredSPO, constraints, encoder, Some("Filter by encoded spatiotemporal info"))).cache
    
    SptRefinement.refineResults(filteredByIdInfo, dfTriples, dfDictionary, constraints)
  }
}
