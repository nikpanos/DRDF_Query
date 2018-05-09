package gr.unipi.datacron.plans.logical.staticPlans.joinSTRange

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.staticPlans.StaticLogicalPlan
import gr.unipi.datacron.plans.logical.staticPlans.sptRefinement.TriplesRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.Map

case class JoinSpatialFirst() extends StaticLogicalPlan() {
  override private[logical] def doExecutePlan(): DataFrame = {
    val transJoinKey = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(AppConfig.getString(qfpJoinKey)))
    val transPredicate = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(AppConfig.getString(qfpJoinTripleP))).get
    val encObj = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(AppConfig.getString(qfpJoinTripleO))).get
    val refinement = TriplesRefinement()

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(
      filterBySubSpatioTemporalInfoParams(DataStore.triplesData, constraints, encoder))

    /*val firstHop = PhysicalPlanner.filterByPO(filterByPOParams(filteredByIdInfo, transJoinKey, None))

    val predicates2 = Map((transPredicate, AppConfig.getString(qfpJoinTripleP)))
    val secondHop = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(firstHop, DataStore.triplesData, tripleObjLongField, predicates2))

    val filteredBySPO = PhysicalPlanner.filterByColumn(filterByColumnParams(secondHop, AppConfig.getString(qfpJoinTripleP), encObj))

    val transFilteredByPO = PhysicalPlanner.decodeColumn(decodeColumnParams(filteredBySPO, AppConfig.getString(qfpJoinTripleP)))

    refinement.refineResults(transFilteredByPO, DataStore.triplesData, constraints)*/
    filteredByIdInfo
  }
}
