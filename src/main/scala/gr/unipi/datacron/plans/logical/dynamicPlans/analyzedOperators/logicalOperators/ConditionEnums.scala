package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators

object ConditionEnums {

  sealed trait ConditionEnum

  case object Eq extends ConditionEnum

  case object Gt extends ConditionEnum

  case object Gte extends ConditionEnum

  case object Lt extends ConditionEnum

  case object Lte extends ConditionEnum

  case object Neq extends ConditionEnum

}
