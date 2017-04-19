package gr.unipi.datacron.plans.logical.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

case class StarRdfFirst(config: Config) extends BaseStar(config) {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val qPredTrans = translateKey(dfDictionary, qPred)
    val qObjTrans = translateKey(dfDictionary, qObj)

    val filteredSPO = PhysicalPlanner.filterByPO(dfTriples, qPredTrans, qObjTrans)
    
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filteredSPO, constraints, encoder).cache
    
    SptRefinement.refineResults(filteredByIdInfo, dfTriples, dfDictionary, constraints)
  }
}
