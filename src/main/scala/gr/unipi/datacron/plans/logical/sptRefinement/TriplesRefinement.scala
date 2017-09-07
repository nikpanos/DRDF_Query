package gr.unipi.datacron.plans.logical.sptRefinement

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

private[logical] case class TriplesRefinement() extends BaseRefinement {

  val encodedUriMBR: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriMBR, Some("Find encoded " + uriMBR))).get
  val encodedUriTime: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriTimeStart, Some("Find encoded " + uriTimeStart))).get

  val encodedUriTemporalFeature: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriHasTemporalFeature, Some("Find encoded " + uriHasTemporalFeature))).get
  val encodedUriGeometry: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriHasGeometry, Some("Find encoded " + uriHasGeometry))).get

  def addSpatialAndTemporalColumns(dfDestination: DataFrame, dfSource: DataFrame): DataFrame = {

    println("first joins (temporal, spatial)")
    val predicates = Map((encodedUriGeometry, tripleGeometryField), (encodedUriTemporalFeature, tripleTemporalField))
    val join1 = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(dfDestination, dfSource, tripleSubLongField, predicates, Some("(Self join)Add encoded geometry and temporalFeature columns")))

    println("second join (spatial)")

    val mbrPredicates = Map((encodedUriMBR, tripleMBRField))
    val join2 = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(join1, DataStore.triplesData, tripleGeometryField, mbrPredicates, Some("(Self join)Add encoded spatial column")))

    println("second join (temporal)")

    val temporalPredicates = Map((encodedUriTime, tripleTimeStartField))
    val result = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(join2, DataStore.triplesData, tripleTemporalField, temporalPredicates, Some("(Self join)Add encoded temporal column")))

    result
  }

  def refineResults(dfFilteredTriples: DataFrame, dfAllTriples: DataFrame, constraints: SpatioTemporalRange): DataFrame = {

    val extendedTriples = if (dfFilteredTriples.hasColumn(tripleMBRField)) dfFilteredTriples else
      addSpatialAndTemporalColumns(dfFilteredTriples, dfAllTriples)

    decodeDatesAndRefineResult(extendedTriples, constraints)
  }
}
