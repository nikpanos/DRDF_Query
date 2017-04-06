package gr.unipi.datacron.plans.logical.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

case class StarRdfFirst(config: Config) extends BaseStar(config) {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredSPO = PhysicalPlanner.filterByPO(dfTriples, qPred, qObj)
    
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(filteredSPO, constraints, encoder).cache
    
    StarRefinement.refineResults(filteredByIdInfo, dfTriples, dfDictionary, constraints)
  }
}
