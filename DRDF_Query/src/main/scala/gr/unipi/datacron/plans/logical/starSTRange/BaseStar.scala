package gr.unipi.datacron.plans.logical.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import org.apache.spark.sql.DataFrame

import scala.util.Try

abstract private[starSTRange] class BaseStar(config: Config) extends BaseLogicalPlan(config) {

  private[starSTRange] val qPred: Option[String] = Try(config.getString(qfpTripleP)).toOption
  private[starSTRange] val qObj: Option[String] = Try(config.getString(qfpTripleO)).toOption

  private def checkAndReturn(x: Option[Long], y: String): Option[Long] = {
    if (x.isEmpty) {
      println("Warning: Query predicate not found: " + y + ". Ignoring it...")
    }
    x
  }

  private[starSTRange] def translateKey(dfDictionary: DataFrame, pred: Option[String]): Option[Long] = {
    if (pred.isDefined) {
      checkAndReturn(PhysicalPlanner.pointSearchKey(dfDictionary, pred.get), pred.get)
    }
    else {
      None
    }
  }
}
