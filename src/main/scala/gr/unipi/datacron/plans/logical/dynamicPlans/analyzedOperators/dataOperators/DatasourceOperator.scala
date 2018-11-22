package gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators

import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.commonOperators.Base0ChildOperator
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.DataFrameUtils._

case class DatasourceOperator(df: DataFrame) extends Base0ChildOperator() {
  val isPropertyTableSource: Boolean = df.isPropertyTable
}
