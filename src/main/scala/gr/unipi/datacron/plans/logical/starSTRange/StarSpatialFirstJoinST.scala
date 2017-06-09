package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

case class StarSpatialFirstJoinST() extends BaseStar() {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(dfTriples, constraints, encoder)

    val extendedTriples = SptRefinement.addSpatialAndTemporalColumns(filteredByIdInfo, filteredByIdInfo, dfDictionary)

    val qPredTrans = translateKey(dfDictionary, qPred)
    val qObjTrans = translateKey(dfDictionary, qObj)
    val filteredSPO = PhysicalPlanner.filterByPO(extendedTriples, qPredTrans, qObjTrans).cache

    SptRefinement.refineResults(filteredSPO, dfTriples, dfDictionary, constraints)
  }
}