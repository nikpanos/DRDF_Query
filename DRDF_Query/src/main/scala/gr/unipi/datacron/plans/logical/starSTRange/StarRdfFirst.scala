package gr.unipi.datacron.plans.logical.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.plans.physical.Executor
import org.apache.spark.sql.DataFrame

case class StarRdfFirst(config: Config) extends BaseStar(config) {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredSPO = Executor.triples.filterByPO(dfTriples, qPred, qObj)
    
    val filteredByIdInfo = Executor.triples.filterBySubSpatioTemporalInfo(filteredSPO, constraints, encoder).cache
    
    StarRefinement.refineResults(filteredByIdInfo, dfTriples, dfDictionary, constraints)
  }
}
