package gr.unipi.datacron.queries.sptRange

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.encoding._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.queries.BaseQuery
import gr.unipi.datacron.operators.Executor
import scala.util.Try

class SpatialFirst(config: Config) extends BaseQuery(config) {
  
  val constraints = new SpatioTemporalRange(
      config.getDouble(qfpLatLower),
      config.getDouble(qfpLonLower),
      config.getDouble(qfpLatUpper),
      config.getDouble(qfpLonUpper),
      config.getLong(qfpTimeLower),
      config.getLong(qfpTimeUpper))
  
  val encoder = new SimpleEncoder(config)
  
  override def executeQuery(): Boolean = {
    import DataStore.spark.implicits._
    
    println("Executing spatial-first spatio-temporal range query")
    
    val filteredByIdInfo = Executor.triples.filterBySubSpatioTemporalInfo(DataStore.triplesData, constraints, encoder)
    
    val filteredSPO = Executor.triples.filterBySPO(filteredByIdInfo,
        Try(config.getLong(qfpTripleS)).toOption,
        Try(config.getLong(qfpTripleP)).toOption,
        Try(config.getLong(qfpTripleO)).toOption).cache
    
    println(filteredSPO.count)
    
    val result = Refinement.refineResults(filteredSPO, constraints)
    result.show()
    println("Result count: " + result.count)
    
    true
  }
}