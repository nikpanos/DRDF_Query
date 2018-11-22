package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.{Base1ChildOperator, BaseOperator}
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators.SortEnums._

case class SortOperator(child: BaseOperator, cols: Array[(String, SortEnum)]) extends Base1ChildOperator(child) {

}
