package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base2ChildOperator, BaseOperator}
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators.LogicalAggregateEnums._

case class LogicalAggregateOperator(leftChild: BaseOperator with BooleanTrait, rightChild: BaseOperator with BooleanTrait, aggregation: LogicalAggregateEnum) extends Base2ChildOperator(leftChild, rightChild) with BooleanTrait {

}
