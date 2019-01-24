package gr.unipi.datacron.plans.logical.staticPlans.starSTRange

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ConditionType
import gr.unipi.datacron.plans.logical.staticPlans.sptRefinement.PropertiesRefinement2
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.{filterByColumnValueParams, filterBySubSpatioTemporalInfoParams}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class StarProperties() extends BaseStar() {
  override private[logical] def doExecutePlan(): DataFrame = {
    val qPredEncoded = encodePredicate(qPred)
    val qObjEncoded = encodePredicate(qObj)

    val refinement = PropertiesRefinement2()

    //val notNullDf = LogicalPlanner.filterNullProperties(filterNullPropertiesParams(DataStore.triplesData))

    val filteredDF = PhysicalPlanner.filterByColumnValue(filterByColumnValueParams(DataStore.triplesData, qPredEncoded.get.toString, qObjEncoded.get, ConditionType.EQ))

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredDF, constraints, encoder))

    refinement.refineResults(filteredByIdInfo, constraints, qPredEncoded.get, qObjEncoded.get)
  }
}
