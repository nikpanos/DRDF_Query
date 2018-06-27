package gr.unipi.datacron.plans.logical.staticPlans.sptRefinement

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

private[logical] case class TriplesRefinement() extends BaseRefinement {

  val encodedUriMBR: Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(uriMBR, None)).get
  val encodedUriTime: Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(uriTimeStart, None)).get

  val encodedUriTemporalFeature: Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(uriHasTemporalFeature, None)).get
  val encodedUriGeometry: Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(uriHasGeometry, None)).get

  def addSpatialAndTemporalColumns(dfDestination: DataFrame, dfSource: DataFrame): DataFrame = {
    val predicates = Map((encodedUriGeometry, tripleGeometryField), (encodedUriTemporalFeature, tripleTemporalField))
    /*val join1 = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(dfDestination, dfSource, tripleSubLongField, predicates, Some("(Self join)Add encoded geometry and temporalFeature columns")))

    val mbrPredicates = Map((encodedUriMBR, tripleMBRField))
    val join2 = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(join1, DataStore.triplesData, tripleGeometryField, mbrPredicates, Some("(Self join)Add encoded spatial column")))

    val temporalPredicates = Map((encodedUriTime, tripleTimeStartField))
    val result = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(join2, DataStore.triplesData, tripleTemporalField, temporalPredicates, Some("(Self join)Add encoded temporal column")))

    result*/
    dfDestination
  }

  def refineResults(dfFilteredTriples: DataFrame, dfAllTriples: DataFrame, constraints: SpatioTemporalRange): DataFrame = {

    val extendedTriples = if (dfFilteredTriples.hasColumn(tripleMBRField)) dfFilteredTriples else
      addSpatialAndTemporalColumns(dfFilteredTriples, dfAllTriples)

    decodeDatesAndRefineResult(extendedTriples, constraints)
  }
}
