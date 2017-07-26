package gr.unipi.datacron.plans.logical.sptRefinement

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

private[logical] case class SptRefinement() {

  val encodedUriMBR: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriMBR, Some("Find encoded " + uriMBR))).get
  val encodedUriTime: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriTimeStart, Some("Find encoded " + uriTimeStart))).get

  val encodedUriTemporalFeature: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriHasTemporalFeature, Some("Find encoded " + uriHasTemporalFeature))).get
  val encodedUriGeometry: Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(uriHasGeometry, Some("Find encoded " + uriHasGeometry))).get

  def addSpatialAndTemporalColumns(dfDestination: DataFrame, dfSource: DataFrame): DataFrame = {

    val predicates = Map((encodedUriGeometry, tripleGeometryField), (encodedUriTemporalFeature, tripleTemporalField))
    val join1 = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(dfDestination, dfSource, tripleSubLongField, predicates, Some("(Self join)Add encoded geometry and temporalFeature columns")))

    val mbrPredicates = Map((encodedUriMBR, tripleMBRField))
    val join2 = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(join1, dfSource, tripleGeometryField, mbrPredicates, Some("(Self join)Add encoded spatial column")))

    val temporalPredicates = Map((encodedUriTime, tripleTimeStartField))
    val result = PhysicalPlanner.joinNewObjects(joinNewObjectsParams(join2, dfSource, tripleTemporalField, temporalPredicates, Some("(Self join)Add encoded temporal column")))

    result
  }

  def refineResults(dfFilteredTriples: DataFrame, dfAllTriples: DataFrame, constraints: SpatioTemporalRange): DataFrame = {

    val extendedTriples = if (dfFilteredTriples.hasColumn(tripleMBRField)) dfFilteredTriples else
      addSpatialAndTemporalColumns(dfFilteredTriples, dfAllTriples)

    val translatedExtendedTriples = PhysicalPlanner.translateColumns(translateColumnsParams(extendedTriples, Array(tripleMBRField, tripleTimeStartField), Some("Add decoded spatial and temporal columns")))

    val result = PhysicalPlanner.filterbySpatioTemporalRange(filterbySpatioTemporalRangeParams(translatedExtendedTriples, constraints, Some("Filter by spatiotemporal columns")))


    //Translate the result before returning
    val outPrepared = PhysicalPlanner.prepareForFinalTranslation(prepareForFinalTranslationParams(result))
    val outTranslated = PhysicalPlanner.translateColumns(translateColumnsParams(outPrepared, Array(tripleSubLongField, triplePredLongField, tripleObjLongField), Some("Final decode of columns")))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)

  }
}
