package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.Base0ChildOperator
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ConditionType

case class ConditionOperator(leftPart: String, condition: ConditionType, rightPart: String) extends Base0ChildOperator() with BooleanTrait {

}
