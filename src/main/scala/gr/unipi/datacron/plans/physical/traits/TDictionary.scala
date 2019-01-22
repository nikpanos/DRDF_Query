package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator
import org.apache.spark.sql.DataFrame

trait TDictionary {
  def decodeSingleKey(params: decodeSingleKeyParams): Option[String]

  def encodeSingleValue(params: encodeSingleValueParams): Option[Long]

  def decodeColumn(params: decodeColumnParams): DataFrame

  def decodeColumns(params: decodeColumnsParams): DataFrame

  def decodeAllColumns(params: decodeAllColumnsParams): DataFrame
}

case class decodeSingleKeyParams(key: Long, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class encodeSingleValueParams(value: String, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class decodeColumnParams(df: DataFrame, columnName: String, preserveColumnName: Boolean = false, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class decodeColumnsParams(df: DataFrame, columnNames: Array[String], preserveColumnNames: Boolean = false, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class decodeAllColumnsParams(df: DataFrame, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams