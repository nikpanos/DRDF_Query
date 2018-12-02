package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.spatiotemporalOperators

import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}

case class ApproximateBoxOperator(child: BaseOperator, constraints: SpatioTemporalRange, prefixed: Boolean) extends Base1ChildOperator(child, prefixed) {

}
