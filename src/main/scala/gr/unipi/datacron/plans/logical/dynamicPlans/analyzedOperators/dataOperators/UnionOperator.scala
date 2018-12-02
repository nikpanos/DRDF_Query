package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{BaseNChildOperator, BaseOperator}

case class UnionOperator(children: Array[BaseOperator], prefixed: Boolean) extends BaseNChildOperator(children, prefixed) {

}
