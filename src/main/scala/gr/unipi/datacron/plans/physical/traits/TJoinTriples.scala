package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TJoinTriples {
  def joinNewObjects(params: joinNewObjectsParams): DataFrame
}

case class joinNewObjectsParams(df: DataFrame, dfTriples: DataFrame, subjectColumn: String, predicates: Map[Long, String],
                                override val operationName: Option[String] = None) extends BaseOperatorParams