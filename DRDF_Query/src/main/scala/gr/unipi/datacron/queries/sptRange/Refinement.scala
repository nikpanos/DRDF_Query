package gr.unipi.datacron.queries.sptRange

import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.operators.Executor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Refinement {
  def refineResults(data: DataFrame, constraints: SpatioTemporalRange): DataFrame = {
    val dictionary = DataStore.dictionaryData

    val encodedUriMBR = Executor.dictionary.pointSearchKey(dictionary, uriMBR).get
    val encodedUriTime = Executor.dictionary.pointSearchKey(dictionary, uriTime).get
    
    val predicates = Map((encodedUriMBR, tripleMBRField), (encodedUriTime, tripleTimeStartField))
    val extendedTriples = Executor.joinTriples.joinSubjectsWithNewObjects(data, DataStore.triplesData, predicates)
    
    val translatedExtendedTriples = Executor.dictionary.translateColumns(extendedTriples, dictionary, Array(tripleMBRField, tripleTimeStartField))

    val result = Executor.triples.filterbySpatioTemporalRange(translatedExtendedTriples, constraints)

    //Translate the result before returning
    val outPrepared = Executor.triples.prepareForFinalTranslation(result)
    val outTranslated = Executor.dictionary.translateColumns(outPrepared, dictionary, Array(tripleSubLongField, triplePredLongField, tripleObjLongField))
    val outColumns = outTranslated.columns.filter(_.endsWith(tripleTranslateSuffix))
    outTranslated.select(outColumns.head, outColumns.tail: _*)
  }
}