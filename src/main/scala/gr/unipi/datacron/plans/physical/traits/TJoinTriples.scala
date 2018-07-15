package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator
import org.apache.spark.sql.DataFrame

trait TJoinTriples {
  def joinDataframes(params: joinDataframesParams): DataFrame
}

case class joinDataframesParams(df1: DataFrame, df2: DataFrame, df1JoinColumn: String, df2JoinColumn: String, df1EstimatedSize: Long, df2EstimatedSize: Long,
                                override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams