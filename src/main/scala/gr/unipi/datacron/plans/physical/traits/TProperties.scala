package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TProperties {
  def addTemporaryColumnForRefinement(params: addTemporaryColumnForRefinementParams): DataFrame
  def filterStarByTemporaryColumn(params: filterStarByTemporaryColumnParams): DataFrame
  def addSpatialAndTemporalColumnsByTemporaryColumn(params: addSpatialAndTemporalColumnsByTemporaryColumnParams): DataFrame
  def filterNullProperties(params: filterNullPropertiesParams): DataFrame
}


case class addTemporaryColumnForRefinementParams(df: DataFrame, predicates: Array[Long], override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterStarByTemporaryColumnParams(df: DataFrame, value: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class addSpatialAndTemporalColumnsByTemporaryColumnParams(df: DataFrame, spatialColumn: Int, temporalColumn: Int, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterNullPropertiesParams(df: DataFrame, override val operationName: Option[String] = None) extends BaseOperatorParams