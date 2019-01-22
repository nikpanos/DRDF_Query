package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator
import org.apache.spark.sql.DataFrame

trait TTriples {
  def filterByColumn(params: filterByColumnParams): DataFrame

  def filterByValue(params: filterByValueParams): DataFrame

  def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame

  def filterBySpatioTemporalRange(params: filterBySpatioTemporalRangeParams): DataFrame

  def filterByMultipleOr(params: filterByMultipleOrParams): DataFrame

  def unionDataframes(params: unionDataframesParams): DataFrame

  def limitResults(params: limitResultsParams): DataFrame

  def sortResults(params: sortResultsParams): DataFrame
}

case class filterByColumnParams(df: DataFrame, columnName: String, value: Any, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterByValueParams(df: DataFrame, value: String, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterBySubSpatioTemporalInfoParams(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder,
                                               override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterBySpatioTemporalRangeParams(df: DataFrame, range: SpatioTemporalRange, spatioTemporalColums: Array[String],
                                             override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterByMultipleOrParams(df: DataFrame, colNamesAndValues: Array[(String, String)],
                                    override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class unionDataframesParams(df1: DataFrame, df2: DataFrame, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class limitResultsParams(df: DataFrame, limitNo: Int, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class sortResultsParams(df: DataFrame, cols: Array[(String, Boolean)], override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams