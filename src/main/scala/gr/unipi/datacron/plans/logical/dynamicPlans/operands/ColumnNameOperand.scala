package gr.unipi.datacron.plans.logical.dynamicPlans.operands
import java.lang

case class ColumnNameOperand(columnName: String) extends BaseOperand{
  override protected def addContentsToStringBuilder(builder: lang.StringBuilder): Unit = builder.append("COLUMNNAME: ").append(columnName)
}
