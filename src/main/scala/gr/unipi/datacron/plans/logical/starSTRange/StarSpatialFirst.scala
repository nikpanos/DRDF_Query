package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

case class StarSpatialFirst() extends BaseStar() {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(dfTriples, constraints, encoder))

    val qPredTrans = encodePredicate(dfDictionary, qPred)
    val qObjTrans = encodePredicate(dfDictionary, qObj)

    val filteredSPO = PhysicalPlanner.filterByPO(filterByPOParams(filteredByIdInfo, qPredTrans, qObjTrans)).cache
    
    SptRefinement.refineResults(filteredSPO, dfTriples, dfDictionary, constraints)
  }
}