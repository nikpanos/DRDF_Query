package gr.unipi.datacron.plans.physical.traits

import org.apache.spark.sql.DataFrame

trait TDictionary {
  def pointSearchValue(key: Long): Option[String]
  def pointSearchKey(value: String): Option[Long]
  def translateColumn(dfTriples: DataFrame, columnName: String): DataFrame
  def translateColumns(dfTriples: DataFrame, columnNames: Array[String]): DataFrame
}