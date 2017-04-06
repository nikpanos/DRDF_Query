package gr.unipi.datacron.queries.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.operators.Executor
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

import scala.util.Try

case class StarSpatialFirst(config: Config) extends BaseStar(config) {
  override def doExecuteQuery(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = Executor.triples.filterBySubSpatioTemporalInfo(dfTriples, constraints, encoder)
    
    val filteredSPO = Executor.triples.filterBySPO(filteredByIdInfo, qSub, qPred, qObj).cache
    
    StarRefinement.refineResults(filteredSPO, dfDictionary, constraints)
  }
}