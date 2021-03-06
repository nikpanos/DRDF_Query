package gr.unipi.datacron.plans.logical.staticPlans.starSTRange

import gr.unipi.datacron.plans.logical.staticPlans.sptRefinement.TriplesRefinement
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class StarRdfFirst() extends BaseStar() {
  override def doExecutePlan(): DataFrame = {

    //val tmp = DataStore.triplesData.filter("predLong=-3").filter("objLong=-14").groupBy("subLong").count().sort(col("count").desc)
    //val tmp1 = PhysicalPlanner.translateColumn(translateColumnParams(tmp, "subLong")).select("subLong_trans", "count")

    //return tmp1

    val qPredTrans = encodePredicate(qPred)
    val qObjTrans = encodePredicate(qObj)

    val refinement = TriplesRefinement()

    /*val filteredSPO = PhysicalPlanner.filterByPO(filterByPOParams(DataStore.triplesData, qPredTrans, qObjTrans, Some("Filter by spo predicate")))

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredSPO, constraints, encoder, Some("Filter by encoded spatiotemporal info"))).cache

    refinement.refineResults(filteredByIdInfo, DataStore.triplesData, constraints)*/
    DataStore.triplesData
  }
}