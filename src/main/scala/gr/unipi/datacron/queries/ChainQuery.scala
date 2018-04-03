package gr.unipi.datacron.queries

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.staticPlans.chainSTRange.ChainSTRange
import gr.unipi.datacron.common.Consts._

case class ChainQuery() extends BaseQuery() {
  protected[queries] override def getExecutionPlan(): Option[BaseLogicalPlan] = AppConfig.getString(qfpLogicalPlans) match {
    case `propertiesChainQueryPlan` => Some(ChainSTRange())
  }
}