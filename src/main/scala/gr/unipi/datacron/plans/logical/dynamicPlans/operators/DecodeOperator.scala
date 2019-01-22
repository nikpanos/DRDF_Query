package gr.unipi.datacron.plans.logical.dynamicPlans.operators

case class DecodeOperator(child: BaseOperator) extends BaseOpW1Child(child) {
  override protected def estimateOutputSize(): Long = child.estimateOutputSize()

  fillAndFormArrayColumns()
}
