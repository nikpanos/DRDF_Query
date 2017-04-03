package gr.unipi.datacron.operators.traits

import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import org.apache.spark.sql.DataFrame

trait TTriples {
  def filterBySPO(df: DataFrame, sub: Option[Long], pred: Option[Long], obj: Option[Long]): DataFrame
  def filterBySubSpatioTemporalInfo(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder): DataFrame
  def pointSearchObject(df: DataFrame, sub: Long, pred: Long): Option[Long]
  def filterbySpatioTemporalRange(df: DataFrame, range: SpatioTemporalRange): DataFrame

  def prepareForFinalTranslation(df: DataFrame): DataFrame
}