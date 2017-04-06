package gr.unipi.datacron.queries.starSTRange

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.operators.Executor
import gr.unipi.datacron.common.DataFrameUtils._
import org.apache.spark.sql.DataFrame

object StarRefinement {

  def addSpatialAndTemporalColumns(dfDestination: DataFrame, dfSource: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val encodedUriMBR = Executor.dictionary.pointSearchKey(dfDictionary, uriMBR).get
    val encodedUriTime = Executor.dictionary.pointSearchKey(dfDictionary, uriTime).get

    val predicates = Map((encodedUriMBR, tripleMBRField), (encodedUriTime, tripleTimeStartField))
    Executor.joinTriples.joinSubjectsWithNewObjects(dfDestination, dfSource, predicates)
  }

  def refineResults(dfFilteredTriples: DataFrame, dfAllTriples: DataFrame, dfDictionary: DataFrame, constraints: SpatioTemporalRange): DataFrame = {

    val extendedTriples = if (dfFilteredTriples.hasColumn(tripleMBRField)) dfFilteredTriples else
      addSpatialAndTemporalColumns(dfFilteredTriples, dfAllTriples, dfDictionary)

    val translatedExtendedTriples = Executor.dictionary.translateColumns(extendedTriples, dfDictionary, Array(tripleMBRField, tripleTimeStartField))

    val result = Executor.triples.filterbySpatioTemporalRange(translatedExtendedTriples, constraints)

    //Translate the result before returning
    val outPrepared = Executor.triples.prepareForFinalTranslation(result)
    val outTranslated = Executor.dictionary.translateColumns(outPrepared, dfDictionary, Array(tripleSubLongField, triplePredLongField, tripleObjLongField))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)
  }
}
