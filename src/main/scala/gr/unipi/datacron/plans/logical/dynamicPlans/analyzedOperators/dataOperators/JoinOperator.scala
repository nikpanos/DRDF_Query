package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base2ChildOperator, BaseOperator}
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators.BooleanTrait

case class JoinOperator(leftChild: BaseOperator, rightChild: BaseOperator, condition: Option[BaseOperator with BooleanTrait]) extends Base2ChildOperator(leftChild, rightChild) {

}
