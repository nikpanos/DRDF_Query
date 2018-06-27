package gr.unipi.datacron.common

import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator

trait BaseOperatorParams {
  def logicalOperator: Option[BaseOperator]
}
