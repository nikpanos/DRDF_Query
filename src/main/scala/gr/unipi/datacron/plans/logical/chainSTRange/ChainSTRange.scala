package gr.unipi.datacron.plans.logical.chainSTRange

import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.DataFrame

case class ChainSTRange() extends BaseLogicalPlan() {
  override private[logical] def doExecutePlan(): DataFrame = {
    val predicate1 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(":hasHeading")).get
    val predicate2 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(":hasSpeed")).get
    val predicate3 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(":occurs")).get
    val predicate4 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(":ofMovingObject")).get

    val vesPredicate1 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams("a")).get
    val vesPredicate2 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(":has_vesselFixingDeviceType")).get
    val vesPredicate3 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(":has_vesselMMSI")).get
    val vesPredicate4 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams("244010219")).get
    val vesPredicate5 = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(":vesselName")).get


    val filteredDF = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(DataStore.nodeData, Array(predicate1.toString, predicate2.toString, predicate4.toString)))

    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(filteredDF, constraints, encoder))

    val filteredByPredicate3 = PhysicalPlanner.filterByPredicateAndRenameObject(filterByPredicateAndRenameObjectParams(DataStore.triplesData, predicate3))

    val joinedDf = PhysicalPlanner.joinDataframes(joinDataframesParams(filteredByPredicate3, filteredByIdInfo, predicate3.toString, tripleSubLongField, "df1", "df2"))

    val vesDF = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(DataStore.vesselData, Array(vesPredicate1.toString, vesPredicate2.toString, vesPredicate5.toString)))

    val filteredVesDF = PhysicalPlanner.filterByColumn(filterByColumnParams(vesDF, vesPredicate3.toString, vesPredicate4))

    val joined2Df = PhysicalPlanner.joinDataframes(joinDataframesParams(joinedDf, filteredVesDF, predicate4.toString, tripleSubLongField, "df1", "df2"))

    joined2Df
  }
}
