package gr.unipi.datacron.plans.logical.starSTRange

import gr.unipi.datacron.plans.logical.sptRefinement.{PropertiesRefinement, PropertiesRefinement2}
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.{filterByColumnParams, filterBySubSpatioTemporalInfoParams, filterNullPropertiesParams}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class StarProperties() extends BaseStar() {
  override private[logical] def doExecutePlan(): DataFrame = {
    val qPredEncoded = encodePredicate(qPred)
    val qObjEncoded = encodePredicate(qObj)

    val refinement = PropertiesRefinement2()

    //val notNullDf = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(DataStore.triplesData))

    val filteredDF = PhysicalPlanner.filterByColumn(filterByColumnParams(DataStore.triplesData, qPredEncoded.get.toString, qObjEncoded.get))

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredDF, constraints, encoder))

    refinement.refineResults(filteredByIdInfo, constraints, qPredEncoded.get, qObjEncoded.get)
  }
}
