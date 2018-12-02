package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.columnOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}

case class ProjectOperator(child: BaseOperator, columnNames: Array[String], prefixed: Boolean) extends Base1ChildOperator(child, prefixed) {

}
