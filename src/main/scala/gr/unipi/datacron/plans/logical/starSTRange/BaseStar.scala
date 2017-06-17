package gr.unipi.datacron.plans.logical.starSTRange


import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.pointSearchKeyParams
import org.apache.spark.sql.DataFrame

import scala.util.Try

abstract private[starSTRange] class BaseStar() extends BaseLogicalPlan() {

  private[starSTRange] val qPred: Option[String] = Try(AppConfig.getString(qfpTripleP)).toOption
  private[starSTRange] val qObj: Option[String] = Try(AppConfig.getString(qfpTripleO)).toOption

  private def checkAndReturn(x: Option[Long], y: String): Option[Long] = {
    if (x.isEmpty) {
      println("Warning: Query predicate not found: " + y + ". Ignoring it...")
    }
    x
  }

  private[starSTRange] def encodePredicate(pred: Option[String]): Option[Long] = {
    if (pred.isDefined) {
      checkAndReturn(PhysicalPlanner.pointSearchKey(pointSearchKeyParams(pred.get, Some("Find encoded predicate: " + pred.get))), pred.get)
    }
    else {
      None
    }
  }
}
