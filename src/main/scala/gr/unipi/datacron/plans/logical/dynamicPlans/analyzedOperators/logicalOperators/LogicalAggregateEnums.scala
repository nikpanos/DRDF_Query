package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators

object LogicalAggregateEnums {

  sealed trait LogicalAggregateEnum

  case object And extends LogicalAggregateEnum

  case object Or extends LogicalAggregateEnum

}
