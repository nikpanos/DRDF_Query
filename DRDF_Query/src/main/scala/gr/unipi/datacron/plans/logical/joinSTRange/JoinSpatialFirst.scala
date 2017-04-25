package gr.unipi.datacron.plans.logical.joinSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.sptRefinement.SptRefinement
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.Map

case class JoinSpatialFirst(config: Config) extends BaseLogicalPlan(config) {
  override private[logical] def doExecutePlan(dfTriples: DataFrame, dfDictionary: DataFrame): DataFrame = {
    val filteredByIdInfo = PhysicalPlanner.filterBySubSpatioTemporalInfo(dfTriples, constraints, encoder)

    val transJoinKey = PhysicalPlanner.pointSearchKey(dfDictionary, config.getString(qfpJoinKey))
    val firstHop = PhysicalPlanner.filterByPO(filteredByIdInfo, transJoinKey, None)
    //val predicates1 = Map((transJoinKey, config.getString(qfpJoinKey)))
    //val firstHop = PhysicalPlanner.joinNewObjects(filteredByIdInfo, dfTriples, tripleSubLongField, predicates1)

    val transPredicate = PhysicalPlanner.pointSearchKey(dfDictionary, config.getString(qfpJoinTripleP)).get
    val predicates2 = Map((transPredicate, config.getString(qfpJoinTripleP)))
    val secondHop = PhysicalPlanner.joinNewObjects(firstHop, dfTriples, tripleObjLongField, predicates2)

    val encObj = PhysicalPlanner.pointSearchKey(dfDictionary, config.getString(qfpJoinTripleO)).get
    val filteredBySPO = PhysicalPlanner.filterByColumn(secondHop, config.getString(qfpJoinTripleP), encObj)

    val transFilteredByPO = PhysicalPlanner.translateColumn(filteredBySPO, dfDictionary, config.getString(qfpJoinTripleP))

    SptRefinement.refineResults(transFilteredByPO, dfTriples, dfDictionary, constraints)
  }
}
