package gr.unipi.datacron.plans.physical.projection

import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.{TProjection, dropColumnsParams, prefixColumnsParams, renameColumnsParams}
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.DataFrameUtils._

case class Projection() extends BasePhysicalPlan with TProjection {
  override def dropColumns(params: dropColumnsParams): DataFrame = params.df.drop(params.colNames: _*)

  override def renameColumns(params: renameColumnsParams): DataFrame = {
    val df = params.df
    val renamedColumns = df.columns.map(c => {
      val valueO = params.oldAndNewColNames.get(c)
      if (valueO.isDefined) {
        df(c).as(valueO.get)
      }
      else {
        df(c)
      }
    })
    df.select(renamedColumns: _*)
  }

  override def prefixColumns(params: prefixColumnsParams): DataFrame = params.df.prefixColumns(params.prefix)
}
