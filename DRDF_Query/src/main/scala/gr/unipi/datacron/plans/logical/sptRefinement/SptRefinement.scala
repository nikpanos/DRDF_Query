package gr.unipi.datacron.plans.logical.sptRefinement

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

private[logical] object SptRefinement {

  def addSpatialAndTemporalColumns(dfDestination: DataFrame, dfSource: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val encodedUriMBR = PhysicalPlanner.pointSearchKey(dfDictionary, uriMBR).get
    val encodedUriTime = PhysicalPlanner.pointSearchKey(dfDictionary, uriTime).get

    val predicates = Map((encodedUriMBR, tripleMBRField), (encodedUriTime, tripleTimeStartField))
    PhysicalPlanner.joinNewObjects(dfDestination, dfSource, tripleSubLongField, predicates)
  }

  def refineResults(dfFilteredTriples: DataFrame, dfAllTriples: DataFrame, dfDictionary: DataFrame, constraints: SpatioTemporalRange): DataFrame = {

    val extendedTriples = if (dfFilteredTriples.hasColumn(tripleMBRField)) dfFilteredTriples else
      addSpatialAndTemporalColumns(dfFilteredTriples, dfAllTriples, dfDictionary)

    val translatedExtendedTriples = PhysicalPlanner.translateColumns(extendedTriples, dfDictionary, Array(tripleMBRField, tripleTimeStartField))

    val result = PhysicalPlanner.filterbySpatioTemporalRange(translatedExtendedTriples, constraints)

    //Translate the result before returning
    val outPrepared = PhysicalPlanner.prepareForFinalTranslation(result)
    val outTranslated = PhysicalPlanner.translateColumns(outPrepared, dfDictionary, Array(/*tripleSubLongField, */triplePredLongField, tripleObjLongField))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)
  }
}
