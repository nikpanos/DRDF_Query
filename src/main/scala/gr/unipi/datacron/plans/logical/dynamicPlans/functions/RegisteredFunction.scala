package gr.unipi.datacron.plans.logical.dynamicPlans.functions

import java.lang

import gr.unipi.datacron.plans.logical.dynamicPlans.operands.BaseOperand
import org.apache.spark.sql.Column

case class RegisteredFunction(functionName: String, args: Array[FunctionArgs], implementation: Array[Column] => Column) extends BaseOperand {
  override protected def addContentsToStringBuilder(builder: lang.StringBuilder): Unit = builder.append(" FUNCTION NAME: ").append(functionName)
}
