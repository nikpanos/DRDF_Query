package gr.unipi.datacron.plans.logical.dynamicPlans.operators
import java.lang

case class PrefixOperator(child: BaseOperator, prefix: String) extends BaseOpW1Child(child) {
  override protected def estimateOutputSize(): Long = getChild.estimateOutputSize()

  override protected def addHeaderStringToStringBuilder(builder: lang.StringBuilder): Unit = builder.append("PREFIX: ").append(prefix)

  fillAndFormArrayColumns()
}
