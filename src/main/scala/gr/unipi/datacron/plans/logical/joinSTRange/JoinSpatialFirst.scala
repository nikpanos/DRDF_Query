package gr.unipi.datacron.plans.logical.joinSTRange

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.Map

case class JoinSpatialFirst() extends BaseLogicalPlan() {
  override private[logical] def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(
      filterBySubSpatioTemporalInfoParams(dfTriples, constraints, encoder))

    val transJoinKey = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(AppConfig.getString(qfpJoinKey)))
    val firstHop = PhysicalPlanner.filterByPO(filterByPOParams(filteredByIdInfo, transJoinKey, None))

    val transPredicate = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(AppConfig.getString(qfpJoinTripleP))).get
    val predicates2 = Map((transPredicate, AppConfig.getString(qfpJoinTripleP)))
    val secondHop = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(firstHop, dfTriples, tripleObjLongField, predicates2))

    val encObj = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(AppConfig.getString(qfpJoinTripleO))).get
    val filteredBySPO = PhysicalPlanner.filterByColumn(filterByColumnParams(secondHop, AppConfig.getString(qfpJoinTripleP), encObj))

    val transFilteredByPO = PhysicalPlanner.translateColumn(translateColumnParams(filteredBySPO, AppConfig.getString(qfpJoinTripleP)))

    SptRefinement.refineResults(transFilteredByPO, dfTriples, dfDictionary, constraints)
  }
}
