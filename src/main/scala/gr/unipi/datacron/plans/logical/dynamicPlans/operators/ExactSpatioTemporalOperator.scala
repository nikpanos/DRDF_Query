package gr.unipi.datacron.plans.logical.dynamicPlans.operators

import java.lang

import gr.unipi.datacron.common.SpatioTemporalRange

case class ExactSpatioTemporalOperator(child: BaseOperator, constraints: SpatioTemporalRange) extends BaseOpW1Child(child) {
  fillAndFormArrayColumns()
  override protected def estimateOutputSize(): Long = child.estimateOutputSize
  override protected def addHeaderStringToStringBuilder(builder: lang.StringBuilder): Unit = builder.append("CONSTRAINTS: ").append(constraints)

}
