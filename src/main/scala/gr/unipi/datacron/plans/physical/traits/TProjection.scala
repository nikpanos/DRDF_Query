package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TProjection {
  def dropColumns(params: dropColumnsParams): DataFrame
  def renameColumns(params: renameColumnsParams): DataFrame
  def prefixColumns(params: prefixColumnsParams): DataFrame
  def selectColumns(params: selectColumnsParams): DataFrame
}

case class dropColumnsParams(df: DataFrame, colNames: Array[String], override val operationName: Option[String] = None) extends BaseOperatorParams
case class renameColumnsParams(df: DataFrame, oldAndNewColNames: Map[String, String], override val operationName: Option[String] = None) extends BaseOperatorParams
case class prefixColumnsParams(df: DataFrame, prefix: String, override val operationName: Option[String] = None) extends BaseOperatorParams
case class selectColumnsParams(df: DataFrame, cols: Array[String], override val operationName: Option[String] = None) extends BaseOperatorParams