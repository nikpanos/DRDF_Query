package gr.unipi.datacron.plans.logical.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

case class StarSpatialFirst(config: Config) extends BaseStar(config) {
  override def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(dfTriples, constraints, encoder)

    val filteredSPO = PhysicalPlanner.filterByPO(filteredByIdInfo, qPred, qObj).cache
    
    StarRefinement.refineResults(filteredSPO, dfTriples, dfDictionary, constraints)
  }
}