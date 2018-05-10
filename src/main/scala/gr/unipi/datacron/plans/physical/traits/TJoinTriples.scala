package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TJoinTriples {
  def joinDataframes(params: joinDataframesParams): DataFrame
}

case class joinDataframesParams(df1: DataFrame, df2: DataFrame, df1JoinColumn: String, df2JoinColumn: String,
                                override val operationName: Option[String] = None) extends BaseOperatorParams