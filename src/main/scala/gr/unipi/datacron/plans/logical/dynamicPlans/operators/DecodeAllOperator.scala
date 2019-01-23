package gr.unipi.datacron.plans.logical.dynamicPlans.operators

case class DecodeAllOperator(child: BaseOperator, exceptForColumns: Array[String]) extends BaseOpW1Child(child) {
  override protected def estimateOutputSize(): Long = child.estimateOutputSize()

  fillAndFormArrayColumns()

  override protected def addHeaderStringToStringBuilder(builder: java.lang.StringBuilder): Unit = {
    builder.append("EXCEPT: [")
    exceptForColumns.foreach(cn => builder.append(cn).append(", "))
    builder.append("]")
  }
}
