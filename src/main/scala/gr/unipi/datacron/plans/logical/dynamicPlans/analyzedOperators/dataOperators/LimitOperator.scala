package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}

case class LimitOperator(child: BaseOperator, howMany: Int) extends Base1ChildOperator(child) {

}
