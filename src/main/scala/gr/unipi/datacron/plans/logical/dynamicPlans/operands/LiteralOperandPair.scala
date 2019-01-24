package gr.unipi.datacron.plans.logical.dynamicPlans.operands
import java.lang

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ConditionType

case class LiteralOperandPair(leftOperand: BaseLiteralOperand, rightOperand: BaseLiteralOperand, condition: ConditionType) extends BaseOperand {
  override protected def addContentsToStringBuilder(builder: lang.StringBuilder): Unit = {
    builder.append("LEFT: [").append(leftOperand).append("] RIGHT: [").append(rightOperand).append("] CONDITION: ").append(condition)
  }
}
