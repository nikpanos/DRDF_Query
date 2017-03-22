package gr.unipi.datacron.queries.sptRange

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import gr.unipi.datacron.queries.operators.RdfOperators
import gr.unipi.datacron.queries.operators.CompositeKeyOperators
import gr.unipi.datacron.store.ExpData
import gr.unipi.datacron.queries.BaseQuery

class RdfFirst(config: Config) extends BaseQuery(config) {
  
  val constraints = new SpatioTemporalConstraints(
      config.getDouble(Consts.qfpLatLower),
      config.getDouble(Consts.qfpLonLower),
      config.getDouble(Consts.qfpLatUpper),
      config.getDouble(Consts.qfpLonUpper),
      config.getLong(Consts.qfpTimeLower),
      config.getLong(Consts.qfpTimeUpper))
  
  val tripleFilter = new SPO(
      config.getString(Consts.qfpTripleS),
      config.getString(Consts.qfpTripleP),
      config.getString(Consts.qfpTripleO))
  
  val encoder = new SimpleEncoder(config)

  override def executeQuery(): Boolean = {
    import ExpData.spark.implicits._

    println("Executing rdf-first spatio-temporal range query")
    
    val filteredSPO = RdfOperators.simpleFilter(ExpData.triplesData, tripleFilter)
    
    val filteredByIdInfo = CompositeKeyOperators.filterBySpatiotemporalInfo(filteredSPO, constraints, encoder)
    filteredByIdInfo.cache // persist in main memory
    //println(filteredByIdInfo.count)
    
    val result = NaiveRefinement.refineResults(filteredByIdInfo, constraints)
    println("Result count: " + result.count)

    true
  }
}
