package gr.unipi.datacron.operators.traits

import org.apache.spark.sql.DataFrame

trait TJoinTriples {
  def joinSubjectsWithNewObjects(df: DataFrame, dfTriples: DataFrame, predicates: Map[Long, String]): DataFrame
}
