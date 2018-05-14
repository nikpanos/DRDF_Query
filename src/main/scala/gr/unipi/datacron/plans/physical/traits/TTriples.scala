package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import org.apache.spark.sql.DataFrame

trait TTriples {
  def filterByColumn(params: filterByColumnParams): DataFrame
  def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame
  def filterbySpatioTemporalRange(params: filterbySpatioTemporalRangeParams): DataFrame
  def filterByMultipleOr(params: filterByMultipleOrParams): DataFrame
  def unionDataframes(params: unionDataframesParams): DataFrame
}

case class filterByColumnParams(df: DataFrame, columnName: String, value: Any, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterBySubSpatioTemporalInfoParams(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder,
                                               override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterbySpatioTemporalRangeParams(df: DataFrame, range: SpatioTemporalRange, spatialColumn: String, temporalColumn: String, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterByMultipleOrParams(df: DataFrame, colNamesAndValues: Array[(String, String)], override val operationName: Option[String] = None) extends BaseOperatorParams
case class unionDataframesParams(df1: DataFrame, df2: DataFrame, override val operationName: Option[String] = None) extends BaseOperatorParams