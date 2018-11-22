package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.columnOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}

case class ProjectOperator(child: BaseOperator, columnNames: Array[String]) extends Base1ChildOperator(child) {

}
