package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.Base0ChildOperator
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators.ConditionEnums._

case class ConditionOperator(leftPart: String, condition: ConditionEnum, rightPart: String) extends Base0ChildOperator() with BooleanTrait {

}
