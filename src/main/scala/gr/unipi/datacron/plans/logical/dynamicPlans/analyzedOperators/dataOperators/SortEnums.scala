package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators

object SortEnums {
  sealed trait SortEnum
  case object Asc extends SortEnum
  case object Desc extends SortEnum
}
