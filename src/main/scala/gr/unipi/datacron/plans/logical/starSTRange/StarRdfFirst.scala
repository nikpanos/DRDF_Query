package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

case class StarRdfFirst() extends BaseStar() {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val qPredTrans = translateKey(dfDictionary, qPred)
    val qObjTrans = translateKey(dfDictionary, qObj)

    val filteredSPO = PhysicalPlanner.filterByPO(dfTriples, qPredTrans, qObjTrans)
    
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filteredSPO, constraints, encoder).cache
    
    SptRefinement.refineResults(filteredByIdInfo, dfTriples, dfDictionary, constraints)
  }
}
