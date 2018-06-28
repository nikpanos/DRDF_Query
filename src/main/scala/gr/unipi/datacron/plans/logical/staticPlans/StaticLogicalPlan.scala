package gr.unipi.datacron.plans.logical.staticPlans

import gr.unipi.datacron.common.{AppConfig, SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.BaseLogicalPlan

abstract class StaticLogicalPlan() extends BaseLogicalPlan() {
  private[staticPlans] val constraints = SpatioTemporalRange(
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), AppConfig.getOptionalDouble(qfpAltLower), AppConfig.getLong(qfpTimeLower)),
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), AppConfig.getOptionalDouble(qfpAltUpper), AppConfig.getLong(qfpTimeUpper)))
}
