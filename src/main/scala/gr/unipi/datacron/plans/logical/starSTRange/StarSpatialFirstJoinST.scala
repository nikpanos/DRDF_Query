package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class StarSpatialFirstJoinST() extends BaseStar() {
  override def doExecutePlan(): DataFrame = {
    val qPredTrans = encodePredicate(qPred)
    val qObjTrans = encodePredicate(qObj)

    val refinement = SptRefinement()

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(DataStore.triplesData, constraints, encoder, Some("Filter by encoded spatiotemporal info")))

    val extendedTriples = refinement.addSpatialAndTemporalColumns(filteredByIdInfo, filteredByIdInfo)

    val filteredSPO = PhysicalPlanner.filterByPO(filterByPOParams(extendedTriples, qPredTrans, qObjTrans, Some("Filter by spo predicate"))).cache

    refinement.refineResults(filteredSPO, null, constraints)
  }
}
