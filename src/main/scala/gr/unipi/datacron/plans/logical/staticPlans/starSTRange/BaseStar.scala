package gr.unipi.datacron.plans.logical.staticPlans.starSTRange


import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.staticPlans.StaticLogicalPlan
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.encodeSingleValueParams

import scala.util.Try

abstract private[starSTRange] class BaseStar() extends StaticLogicalPlan() {

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
      checkAndReturn(PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(pred.get, None)), pred.get)
    }
    else {
      None
    }
  }
}
