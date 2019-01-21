package gr.unipi.datacron.plans.logical.dynamicPlans.operators

case class PrefixOperator(child: BaseOperator, prefix: String) extends BaseOpW1Child(child) {
  override protected def estimateOutputSize(): Long = -1
}
