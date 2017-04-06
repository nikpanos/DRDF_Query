package gr.unipi.datacron.plans.logical.starSTRange

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.plans.logical.BaseLogicalPlan

import scala.util.Try

abstract private[starSTRange] class BaseStar(config: Config) extends BaseLogicalPlan(config) {
  val constraints = new SpatioTemporalRange(
    config.getDouble(qfpLatLower),
    config.getDouble(qfpLonLower),
    config.getDouble(qfpLatUpper),
    config.getDouble(qfpLonUpper),
    config.getLong(qfpTimeLower),
    config.getLong(qfpTimeUpper))

  private[starSTRange] val qSub: Option[Long] = Try(config.getLong(qfpTripleS)).toOption
  private[starSTRange] val qPred: Option[Long] = Try(config.getLong(qfpTripleP)).toOption
  private[starSTRange] val qObj: Option[Long] = Try(config.getLong(qfpTripleO)).toOption

  val encoder = new SimpleEncoder(config)
}
