package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TJoinTriples {
  def joinNewObjects(params: joinNewObjectsParams): DataFrame
  def joinDataframes(params: joinDataframesParams): DataFrame
}

case class joinNewObjectsParams(df: DataFrame, dfTriples: DataFrame, subjectColumn: String, predicates: Map[Long, String],
                                override val operationName: Option[String] = None) extends BaseOperatorParams
case class joinDataframesParams(df1: DataFrame, df2: DataFrame, df1JoinColumn: String, df2JoinColumn: String,
                                df1Alias: String, df2Alias: String, override val operationName: Option[String] = None) extends BaseOperatorParams