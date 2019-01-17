package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzers.PlanAnalyzer.getConditionOperatorFromOperandPair
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.{BaseOperator, SelectOperator}
import org.apache.spark.sql.DataFrame

abstract class BaseAnalyzer {
  protected def processNode(node: BaseOperator, dfO: Option[DataFrame]): BaseOperator

  protected def createSelectOperator(so: SelectOperator, child: BaseOperator): SelectOperator = {
    val filters = so.getFilters
    val condition = if (filters.length == 1) {
      getConditionOperatorFromOperandPair(filters(0))
    }
    else {
      val first = filters.head

      val conditionFirst: analyzedOperators.commonOperators.BaseOperator with BooleanTrait = getConditionOperatorFromOperandPair(first)
      filters.tail.foldLeft(conditionFirst)((conditionLeft, op) => {
        val conditionRight = getConditionOperatorFromOperandPair(op)
        analyzedOperators.logicalOperators.LogicalAggregateOperator(conditionLeft, conditionRight, LogicalAggregateEnums.And)
      })
    }
    analyzedOperators.dataOperators.SelectOperator(child, condition, child.isPrefixed)
  }
}
