package gr.unipi.datacron.plans.physical.traits

import org.apache.spark.sql.DataFrame

trait TJoinTriples {
  def joinSubjectsWithNewObjects(df: DataFrame, dfTriples: DataFrame, predicates: Map[Long, String]): DataFrame
}
