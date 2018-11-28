package gr.unipi.datacron.plans.logical.dynamicPlans

import gr.unipi.datacron.common.Consts.qfpEnableFilterByEncodedInfo
import gr.unipi.datacron.common.{AppConfig, SpatioTemporalRange}
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.BaseOperator

class SpatiotemporalPlanAnalyzer(constraints: SpatioTemporalRange) extends PlanAnalyzer {

  private def addApproximateSpatiotemporalOperator(node: BaseOperator): BaseOperator = {
    node match {
      case _ => throw new Exception("Not supported operator")
    }
  }

  override def analyzePlan(root: operators.BaseOperator): BaseOperator = {
    val analyzedPlan = super.analyzePlan(root)
    if (AppConfig.getOptionalBoolean(qfpEnableFilterByEncodedInfo).getOrElse(true)) {

    }
    analyzedPlan
  }
}
