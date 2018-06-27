package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common.BaseOperatorParams
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator
import org.apache.spark.sql.DataFrame

trait TProperties {
  def addTemporaryColumnForRefinement(params: addTemporaryColumnForRefinementParams): DataFrame
  def addSpatialAndTemporalColumnsByTemporaryColumn(params: addSpatialAndTemporalColumnsByTemporaryColumnParams): DataFrame
  def filterNullProperties(params: filterNullPropertiesParams): DataFrame
}


case class addTemporaryColumnForRefinementParams(df: DataFrame, predicates: Array[Long], override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams
case class addSpatialAndTemporalColumnsByTemporaryColumnParams(df: DataFrame, spatialColumn: Int, temporalColumn: Int, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams
case class filterNullPropertiesParams(df: DataFrame, columnNames: Array[String], override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams