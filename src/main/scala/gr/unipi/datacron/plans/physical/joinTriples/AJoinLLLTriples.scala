package gr.unipi.datacron.plans.physical.joinTriples

import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.{TJoinTriples, joinDataframesParams}
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.DataFrameUtils._

case class AJoinLLLTriples() extends BasePhysicalPlan with TJoinTriples {

  override def joinDataframes(params: joinDataframesParams): DataFrame = {
    val alias1 = params.df1Alias.getOrElse("")
    val alias2 = params.df2Alias.getOrElse("")
    val df1 = if (params.df1Alias.isDefined) params.df1.prefixColumns(alias1)
              else params.df1
    val df2 = if (params.df2Alias.isDefined) params.df2.prefixColumns(alias2)
              else params.df2
    df1.join(df2, df1(sanitize(alias1 + params.df1JoinColumn)) === df2(sanitize(alias2 + params.df2JoinColumn)))
    //df1
  }
}
