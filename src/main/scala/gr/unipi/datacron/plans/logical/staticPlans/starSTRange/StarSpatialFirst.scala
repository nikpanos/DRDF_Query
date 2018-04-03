package gr.unipi.datacron.plans.logical.staticPlans.starSTRange

import gr.unipi.datacron.plans.logical.staticPlans.sptRefinement.TriplesRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class StarSpatialFirst() extends BaseStar() {
  override def doExecutePlan(): DataFrame = {
    val qPredTrans = encodePredicate(qPred)
    val qObjTrans = encodePredicate(qObj)

    val refinement = TriplesRefinement()

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(DataStore.triplesData, constraints, encoder))

    val filteredSPO = PhysicalPlanner.filterByPO(filterByPOParams(filteredByIdInfo, qPredTrans, qObjTrans)).cache

    refinement.refineResults(filteredSPO, DataStore.triplesData, constraints)
  }
}