package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import org.apache.spark.sql.DataFrame

trait TTriples {
  def filterByPO(params: filterByPOParams): DataFrame
  def filterByPOandKeepSpatioTemporal(params: filterByPOandKeepSpatioTemporalParams): DataFrame
  def filterByColumn(params: filterByColumnParams): DataFrame
  def filterByPredicateAndRenameObject(params: filterByPredicateAndRenameObjectParams): DataFrame
  def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame
  def pointSearchObject(params: pointSearchObjectParams): Option[Long]
  def filterbySpatioTemporalRange(params: filterbySpatioTemporalRangeParams): DataFrame
  def prepareForFinalTranslation(params: prepareForFinalTranslationParams): DataFrame
}

case class filterByPOParams(df: DataFrame, pred: Option[Long], obj: Option[Long], override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterByPOandKeepSpatioTemporalParams(df: DataFrame, pred: Option[Long], obj: Option[Long], predTemporal: Long, predSpatial: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterByColumnParams(df: DataFrame, columnName: String, value: Any, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterByPredicateAndRenameObjectParams(df: DataFrame, predicateValue: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterBySubSpatioTemporalInfoParams(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder,
                                               override val operationName: Option[String] = None) extends BaseOperatorParams
case class pointSearchObjectParams(df: DataFrame, sub: Long, pred: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterbySpatioTemporalRangeParams(df: DataFrame, range: SpatioTemporalRange, spatialColumn: String, temporalColumn: String, override val operationName: Option[String] = None) extends BaseOperatorParams
case class prepareForFinalTranslationParams(df: DataFrame, override val operationName: Option[String] = None) extends BaseOperatorParams