package gr.unipi.datacron.operators.traits

import org.apache.spark.sql.DataFrame

trait TJoinDictionaryTriples {
  def translateColumn(dfTriples: DataFrame, dfDictionary: DataFrame, columnName: String): DataFrame
  def translateColumns(dfTriples: DataFrame, dfDictionary: DataFrame, columnNames: Array[String]): DataFrame
}