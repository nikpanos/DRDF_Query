package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.columnOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}

case class RenameOperator(child: BaseOperator, columnMapping: Array[(String, String)]) extends Base1ChildOperator(child) {

}
