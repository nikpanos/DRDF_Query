package gr.unipi.datacron.queries.starSTRange

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.operators.Executor
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

object StarRefinement {
  def refineResults(dfTriples: DataFrame, dfDictionary: DataFrame, constraints: SpatioTemporalRange): DataFrame = {
    val encodedUriMBR = Executor.dictionary.pointSearchKey(dfDictionary, uriMBR).get
    val encodedUriTime = Executor.dictionary.pointSearchKey(dfDictionary, uriTime).get

    val predicates = Map((encodedUriMBR, tripleMBRField), (encodedUriTime, tripleTimeStartField))
    val extendedTriples = Executor.joinTriples.joinSubjectsWithNewObjects(dfTriples, DataStore.triplesData, predicates)

    val translatedExtendedTriples = Executor.dictionary.translateColumns(extendedTriples, dfDictionary, Array(tripleMBRField, tripleTimeStartField))

    val result = Executor.triples.filterbySpatioTemporalRange(translatedExtendedTriples, constraints)

    //Translate the result before returning
    val outPrepared = Executor.triples.prepareForFinalTranslation(result)
    val outTranslated = Executor.dictionary.translateColumns(outPrepared, dfDictionary, Array(tripleSubLongField, triplePredLongField, tripleObjLongField))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)
  }
}
