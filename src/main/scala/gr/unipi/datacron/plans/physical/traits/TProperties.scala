package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import org.apache.spark.sql.DataFrame

trait TProperties {
  def addTemporaryColumnForRefinement(params: addTemporaryColumnForRefinementParams): DataFrame
  def addSpatialAndTemporalColumnsByTemporaryColumn(params: addSpatialAndTemporalColumnsByTemporaryColumnParams): DataFrame
  def filterStarByTemporaryColumn(params: filterStarByTemporaryColumnParams): DataFrame
  def filterNullProperties(params: filterNullPropertiesParams): DataFrame
  def filterByProperty(params: filterByPropertyParams): DataFrame
  def addColumnByProperty(params: addColumnByPropertyParams): DataFrame
  def addColumnsByProperty(params: addColumnsByPropertyParams): DataFrame
}


case class addTemporaryColumnForRefinementParams(df: DataFrame, predicates: Array[Long], override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterStarByTemporaryColumnParams(df: DataFrame, value: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class addSpatialAndTemporalColumnsByTemporaryColumnParams(df: DataFrame, spatialColumn: Int, temporalColumn: Int, override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterNullPropertiesParams(df: DataFrame, columnNames: Array[String], override val operationName: Option[String] = None) extends BaseOperatorParams
case class filterByPropertyParams(df: DataFrame, predicateValue: Long, objectValue: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class addColumnByPropertyParams(df: DataFrame, columnName: String, predicateValue: Long, override val operationName: Option[String] = None) extends BaseOperatorParams
case class addColumnsByPropertyParams(df: DataFrame, namesAndPredicates: Array[(String, Long)], override val operationName: Option[String] = None) extends BaseOperatorParams