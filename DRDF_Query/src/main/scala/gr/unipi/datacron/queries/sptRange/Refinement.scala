package gr.unipi.datacron.queries.sptRange

import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.operators.Executor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object Refinement {
  def refineResults(data: DataFrame, constraints: SpatioTemporalRange): DataFrame = {
    import DataStore.spark.implicits._
    
    val dictionary = DataStore.dictionaryData
    
    val encodedUriMBR = Executor.dictionary.pointSearchKey(dictionary, uriMBR).get
    val encodedUriTime = Executor.dictionary.pointSearchKey(dictionary, uriTime).get
    
    val predicates = Map((encodedUriMBR, tripleMBRField), (encodedUriTime, tripleTimeStartField))
    val extendedTriples = Executor.triples.joinSubjectsWithNewObjects(data, DataStore.triplesData, predicates)
    
    val translatedExtendedTriples = Executor.joinDictionaryTriples.translateColumns(extendedTriples, dictionary, Array(tripleMBRField, tripleTimeStartField))
    translatedExtendedTriples.show()
    
    return Executor.triples.filterbySpatioTemporalRange(translatedExtendedTriples, constraints)
    /*val idsForTemporalRefinement = data.filter(flt(0)($"pruneKey")).select($"subject").map(r => (r(0).asInstanceOf[Long], encodedUriTime)).collect()
    val idsForSpatialRefinement = data.filter(flt(1)($"pruneKey")).select($"subject").map(r => (r(0).asInstanceOf[Long], encodedUriMBR)).collect()
    
    val soForTemporalRefinementEncoded = DataStore.triples.getListOByListSP(idsForTemporalRefinement)
    //println(soForTemporalRefinementEncoded.size)
    val soForSpatialRefinementEncoded = DataStore.triples.getListOByListSP(idsForSpatialRefinement)
    //println(soForSpatialRefinementEncoded.size)
    
    val soForTemporalRefinementDecoded = DataStore.dictionary.getValuesListByKeysList(soForTemporalRefinementEncoded)
    //println(soForTemporalRefinementDecoded.size)
    val soForSpatialRefinementDecoded = DataStore.dictionary.getValuesListByKeysList(soForSpatialRefinementEncoded)
    //println(soForSpatialRefinementDecoded.size)
    
    return getResults(constraints, data, soForTemporalRefinementDecoded, soForSpatialRefinementDecoded)*/
  }
}