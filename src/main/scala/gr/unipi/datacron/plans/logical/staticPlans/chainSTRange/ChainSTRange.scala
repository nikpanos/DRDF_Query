package gr.unipi.datacron.plans.logical.staticPlans.chainSTRange

import gr.unipi.datacron.plans.logical.staticPlans.StaticLogicalPlan
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class ChainSTRange() extends StaticLogicalPlan() {
  override private[logical] def doExecutePlan(): DataFrame = {
    val predicate1 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(":hasHeading")).get
    val predicate2 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(":hasSpeed")).get
    val predicate3 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(":occurs")).get
    val predicate4 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(":ofMovingObject")).get

    val vesPredicate1 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams("a")).get
    val vesPredicate2 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(":has_vesselFixingDeviceType")).get
    val vesPredicate3 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(":has_vesselMMSI")).get
    val vesPredicate4 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams("244010219")).get
    val vesPredicate5 = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(":vesselName")).get


    val filteredDF = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(DataStore.nodeData, Array(predicate1.toString, predicate2.toString, predicate4.toString)))

    /*val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredDF, constraints, encoder))

    val filteredByPredicate3 = PhysicalPlanner.filterByPredicateAndRenameObject(filterByPredicateAndRenameObjectParams(DataStore.triplesData, predicate3))

    val joinedDf = PhysicalPlanner.joinDataframes(joinDataframesParams(filteredByPredicate3, filteredByIdInfo, predicate3.toString, tripleSubLongField, Option("df1"), Option("df2")))

    val vesDF = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(DataStore.vesselData, Array(vesPredicate1.toString, vesPredicate2.toString, vesPredicate5.toString)))

    val filteredVesDF = PhysicalPlanner.filterByColumn(filterByColumnParams(vesDF, vesPredicate3.toString, vesPredicate4))

    val joined2Df = PhysicalPlanner.joinDataframes(joinDataframesParams(joinedDf, filteredVesDF, predicate4.toString, tripleSubLongField, Option("df1"), Option("df2")))

    joined2Df*/
    filteredDF
  }
}
