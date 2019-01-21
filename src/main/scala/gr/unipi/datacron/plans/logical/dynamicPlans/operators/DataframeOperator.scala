package gr.unipi.datacron.plans.logical.dynamicPlans.operators

import org.apache.spark.sql.DataFrame

case class DataframeOperator(df: DataFrame) extends BaseOperator {
  override protected def estimateOutputSize(): Long = -1
}
