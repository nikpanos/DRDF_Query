package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TDictionary {
  def decodeSingleKey(params: decodeSingleKeyParams): Option[String]
  def encodeSingleValue(params: encodeSingleValueParams): Option[Long]
  def decodeColumn(params: decodeColumnParams): DataFrame
  def decodeColumns(params: decodeColumnsParams): DataFrame
}

case class decodeSingleKeyParams(key: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class encodeSingleValueParams(value: String, override val operationName: Option[String] = None) extends BaseOperatorParams
case class decodeColumnParams(dfTriples: DataFrame, columnName: String, preserveColumnName: Boolean = false, override val operationName: Option[String] = None) extends BaseOperatorParams
case class decodeColumnsParams(dfTriples: DataFrame, columnNames: Array[String], preserveColumnNames: Boolean = false, override val operationName: Option[String] = None) extends BaseOperatorParams