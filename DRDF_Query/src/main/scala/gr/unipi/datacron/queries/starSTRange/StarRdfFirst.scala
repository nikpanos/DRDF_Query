package gr.unipi.datacron.queries.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.operators.Executor
import org.apache.spark.sql.DataFrame

case class StarRdfFirst(config: Config) extends BaseStar(config) {
  override def doExecuteQuery(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredSPO = Executor.triples.filterByPO(dfTriples, qPred, qObj)
    
    val filteredByIdInfo = Executor.triples.filterBySubSpatioTemporalInfo(filteredSPO, constraints, encoder).cache
    
    StarRefinement.refineResults(filteredByIdInfo, dfTriples, dfDictionary, constraints)
  }
}
