package gr.unipi.datacron.queries.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.operators.Executor
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

import scala.util.Try

case class StarRdfFirst(config: Config) extends BaseStar(config) {
  override def doExecuteQuery(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredSPO = Executor.triples.filterBySPO(dfTriples, qSub, qPred, qObj)
    
    val filteredByIdInfo = Executor.triples.filterBySubSpatioTemporalInfo(filteredSPO, constraints, encoder).cache
    
    StarRefinement.refineResults(filteredByIdInfo, dfDictionary, constraints)
  }
}
