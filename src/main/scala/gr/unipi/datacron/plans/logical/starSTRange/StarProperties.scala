package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.PropertiesRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.{filterBySubSpatioTemporalInfoParams, filterNullPropertiesParams}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class StarProperties() extends BaseStar() {
  override private[logical] def doExecutePlan(): DataFrame = {
    val qPredEncoded = encodePredicate(qPred)
    val qObjEncoded = encodePredicate(qObj)

    val refinement = PropertiesRefinement()

    val notNullDf = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(DataStore.triplesData))

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(notNullDf, constraints, encoder))

    refinement.refineResults(filteredByIdInfo, constraints, qPredEncoded.get, qObjEncoded.get)
  }
}
