package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import org.apache.spark.sql.DataFrame

trait TTriples {
  def filterByPO(df: DataFrame, pred: Option[Long], obj: Option[Long]): DataFrame
  def filterByColumn(df: DataFrame, columnName: String, value: Any): DataFrame
  def filterBySubSpatioTemporalInfo(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder): DataFrame
  def pointSearchObject(df: DataFrame, sub: Long, pred: Long): Option[Long]
  def filterbySpatioTemporalRange(df: DataFrame, range: SpatioTemporalRange): DataFrame

  def prepareForFinalTranslation(df: DataFrame): DataFrame
}