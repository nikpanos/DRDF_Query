package gr.unipi.datacron.plans.logical.fixedPlans.starSTRange

import gr.unipi.datacron.plans.logical.fixedPlans.sptRefinement.TriplesRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.{filterByPOParams, filterByPOandKeepSpatioTemporalParams, filterBySubSpatioTemporalInfoParams}
import gr.unipi.datacron.store.DataStore

case class StarRdfFirstBest() extends BaseStar() {
  override private[logical] def doExecutePlan() = {
    val qPredTrans = encodePredicate(qPred)
    val qObjTrans = encodePredicate(qObj)

    val refinement = TriplesRefinement()

    val filteredSPO = PhysicalPlanner.filterByPOandKeepSpatioTemporal(filterByPOandKeepSpatioTemporalParams(DataStore.triplesData, qPredTrans, qObjTrans, refinement.encodedUriTemporalFeature, refinement.encodedUriGeometry, Some("Filter by spo predicate and spatiotemporal columns")))

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredSPO, constraints, encoder, Some("Filter by encoded spatiotemporal info"))).cache

    val filteredSPOFinal = PhysicalPlanner.filterByPO(filterByPOParams(filteredByIdInfo, qPredTrans, qObjTrans, Some("Filter by spo predicate")))

    refinement.refineResults(filteredSPOFinal, filteredByIdInfo, constraints)
  }
}
