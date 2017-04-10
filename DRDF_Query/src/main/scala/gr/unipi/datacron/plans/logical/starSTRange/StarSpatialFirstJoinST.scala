package gr.unipi.datacron.plans.logical.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

case class StarSpatialFirstJoinST(config: Config) extends BaseStar(config) {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(dfTriples, constraints, encoder)

    val extendedTriples = StarRefinement.addSpatialAndTemporalColumns(filteredByIdInfo, filteredByIdInfo, dfDictionary)

    val qPredTrans = translateKey(dfDictionary, qPred)
    val qObjTrans = translateKey(dfDictionary, qObj)
    val filteredSPO = PhysicalPlanner.filterByPO(extendedTriples, qPredTrans, qObjTrans).cache

    StarRefinement.refineResults(filteredSPO, dfTriples, dfDictionary, constraints)
  }
}
