package gr.unipi.datacron.plans.logical.dynamicPlans.operators

import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.DataFrameUtils._

case class DatasourceOperator(df: DataFrame, isPropertyTableSource: Boolean) extends BaseOpW0Child {
  override protected def estimateOutputSize(): Long = -1

  def hasColumn(colName: String): Boolean = df.hasColumn(colName)
  //def isPropertyTableSource: Boolean = df.isPropertyTable
}
