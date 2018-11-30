package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.columnOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}

case class RenameOperator(child: BaseOperator, columnMapping: Array[(String, String)], prefixed: Boolean) extends Base1ChildOperator(child, prefixed) {

}
