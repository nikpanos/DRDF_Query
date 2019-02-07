package gr.unipi.datacron.plans.physical.traits

import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ConditionType
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.LiteralOperandPair
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator
import org.apache.spark.sql.{Column, DataFrame}

trait TTriples {
  def filterByColumnValue(params: filterByColumnValueParams): DataFrame

  def filterByColumn(params: filterByColumnParams): DataFrame

  def filterByValue(params: filterByValueParams): DataFrame

  def filterByLiteralOperandPair(params: filterByLiteralOperandPairParams): DataFrame

  def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame

  def filterBySpatioTemporalRange(params: filterBySpatioTemporalRangeParams): DataFrame

  def filterByMultipleOr(params: filterByMultipleOrParams): DataFrame

  def unionDataframes(params: unionDataframesParams): DataFrame

  def limitResults(params: limitResultsParams): DataFrame

  def sortResults(params: sortResultsParams): DataFrame

  def distinctData(params: distinctDataParams): DataFrame

  def filterByFunction(params: filterByFunctionParams): DataFrame
}

case class filterByColumnValueParams(df: DataFrame, columnName: String, value: Any, conditionType: ConditionType, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterByColumnParams(df: DataFrame, columnName: String, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterByValueParams(df: DataFrame, value: String, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterByLiteralOperandPairParams(df: DataFrame, operand: LiteralOperandPair, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterBySubSpatioTemporalInfoParams(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder,
                                               override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterBySpatioTemporalRangeParams(df: DataFrame, range: SpatioTemporalRange, spatioTemporalColums: Array[String],
                                             override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterByMultipleOrParams(df: DataFrame, colNamesAndValues: Array[(String, String)],
                                    override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class unionDataframesParams(df1: DataFrame, df2: DataFrame, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class limitResultsParams(df: DataFrame, limitNo: Int, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class sortResultsParams(df: DataFrame, cols: Array[(String, Boolean)], override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class distinctDataParams(df: DataFrame, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams

case class filterByFunctionParams(df: DataFrame, c: Column, override val logicalOperator: Option[BaseOperator] = None) extends BaseOperatorParams