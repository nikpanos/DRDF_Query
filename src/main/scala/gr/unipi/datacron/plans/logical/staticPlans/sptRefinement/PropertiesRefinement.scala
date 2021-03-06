package gr.unipi.datacron.plans.logical.staticPlans.sptRefinement

import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

private[logical] case class PropertiesRefinement() extends BaseRefinement {

  //val encodedUriSpatialShortcut: Long = LogicalPlanner.pointSearchKey(pointSearchKeyParams(uriSpatialShortcut, Some("Find encoded " + uriSpatialShortcut))).get
  //val encodedUriTemporalShortcut: Long = LogicalPlanner.pointSearchKey(pointSearchKeyParams(uriTemporalShortcut, Some("Find encoded " + uriTemporalShortcut))).get

  def refineResults(dfFilteredTriples: DataFrame, constraints: SpatioTemporalRange, qPredEncoded: Long, qObjEncoded: Long): DataFrame = {
    val predArray = Array(qPredEncoded)
    val dfWithTemporalColumn = PhysicalPlanner.addTemporaryColumnForRefinement(addTemporaryColumnForRefinementParams(dfFilteredTriples, predArray))

    /*val dfFilteredBySPO = PhysicalPlanner.filterStarByTemporaryColumn(filterStarByTemporaryColumnParams(dfWithTemporalColumn, qObjEncoded))

    //val dfWithSpatialAndTemporal = LogicalPlanner.addSpatialAndTemporalColumnsByTemporaryColumn(addSpatialAndTemporalColumnsByTemporaryColumnParams(dfFilteredBySPO, 1, 2))

    decodeDatesAndRefineResult(dfFilteredBySPO, constraints)*/
    dfWithTemporalColumn
  }
}
