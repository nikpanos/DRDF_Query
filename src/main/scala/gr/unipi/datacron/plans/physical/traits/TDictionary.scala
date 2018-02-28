package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TDictionary {
  def pointSearchValue(params: pointSearchValueParams): Option[String]
  def pointSearchKey(params: pointSearchKeyParams): Option[Long]
  def decodeColumn(params: decodeColumnParams): DataFrame
  def decodeColumns(params: decodeColumnsParams): DataFrame
}

case class pointSearchValueParams(key: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class pointSearchKeyParams(value: String, override val operationName: Option[String] = None) extends BaseOperatorParams
case class decodeColumnParams(dfTriples: DataFrame, columnName: String, override val operationName: Option[String] = None) extends BaseOperatorParams
case class decodeColumnsParams(dfTriples: DataFrame, columnNames: Array[String], override val operationName: Option[String] = None) extends BaseOperatorParams