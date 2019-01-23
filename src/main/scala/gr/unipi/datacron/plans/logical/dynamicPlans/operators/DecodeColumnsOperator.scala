package gr.unipi.datacron.plans.logical.dynamicPlans.operators

case class DecodeColumnsOperator(child: BaseOperator, columnNames: Array[String]) extends BaseOpW1Child(child) {
  override protected def estimateOutputSize(): Long = child.estimateOutputSize()

  fillAndFormArrayColumns()

  override protected def addHeaderStringToStringBuilder(builder: java.lang.StringBuilder): Unit = {
    builder.append("COLUMNNAMES: [")
    columnNames.foreach(cn => builder.append(cn).append(", "))
    builder.append("]")
  }
}
