package gr.unipi.datacron.plans.physical.traits

import org.apache.spark.sql.DataFrame

trait TJoinTriples {
  def joinNewObjects(df: DataFrame, dfTriples: DataFrame, subjectColumn: String, predicates: Map[Long, String]): DataFrame
}
