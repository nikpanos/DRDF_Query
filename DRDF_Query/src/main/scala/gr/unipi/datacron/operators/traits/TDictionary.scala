package gr.unipi.datacron.operators.traits

import org.apache.spark.sql.DataFrame

trait TDictionary {
  def pointSearchValue(df: DataFrame, key: Long): Option[String]
  def pointSearchKey(df: DataFrame, value: String): Option[Long]
}