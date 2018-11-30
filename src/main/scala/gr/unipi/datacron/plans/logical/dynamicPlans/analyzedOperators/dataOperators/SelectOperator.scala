package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators.BooleanTrait

case class SelectOperator(child: BaseOperator, condition: BooleanTrait, prefixed: Boolean) extends Base1ChildOperator(child, prefixed) {
}
