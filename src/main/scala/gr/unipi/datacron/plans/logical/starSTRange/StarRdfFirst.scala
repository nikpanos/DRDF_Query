package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class StarRdfFirst() extends BaseStar() {
  override def doExecutePlan(): DataFrame = {
    val qPredTrans = encodePredicate(qPred)
    val qObjTrans = encodePredicate(qObj)

    val refinement = SptRefinement()

    val filteredSPO = PhysicalPlanner.filterByPO(filterByPOParams(DataStore.triplesData, qPredTrans, qObjTrans, Some("Filter by spo predicate")))
    
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredSPO, constraints, encoder, Some("Filter by encoded spatiotemporal info"))).cache

    refinement.refineResults(filteredByIdInfo, DataStore.triplesData, constraints)
  }
}
