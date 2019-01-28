package gr.unipi.datacron.plans.physical.joinTriples

import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.{TJoinTriples, joinAllDataframesParams, joinDataframesParams}
import gr.unipi.datacron.common.Utils._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.lit

case class AutoJoin() extends BasePhysicalPlan with TJoinTriples {

  private def doJoinDataframes(leftDf: DataFrame, rightDf: DataFrame, col: Column): DataFrame =
    leftDf.join(rightDf, col)

  override def joinDataframes(params: joinDataframesParams): DataFrame = {
    /*val alias1 = params.df1Alias.getOrElse("")
    val alias2 = params.df2Alias.getOrElse("")
    val df1 = if (params.df1Alias.isDefined) params.df1.prefixColumns(alias1)
              else params.df1
    val df2 = if (params.df2Alias.isDefined) params.df2.prefixColumns(alias2)
              else params.df2*/

    /*val thres = AppConfig.getOptionalLong(qfpBroadcastThreshold)
    if (thres.isDefined && (params.df1EstimatedSize <= thres.get)) {
      //println("Forcing broadcast join on df1: " + params.df1EstimatedSize)
      params.df2.join(broadcast(params.df1), params.df1(sanitize(params.df1JoinColumn)) === params.df2(sanitize(params.df2JoinColumn)))
    }
    else if (thres.isDefined && (params.df2EstimatedSize <= thres.get)) {
      //println("Forcing broadcast join on df2: " + params.df2EstimatedSize)
      params.df1.join(broadcast(params.df2), params.df1(sanitize(params.df1JoinColumn)) === params.df2(sanitize(params.df2JoinColumn)))
    }
    else {
      //println("No forced broadcast join")
      params.df1.join(params.df2, params.df1(sanitize(params.df1JoinColumn)) === params.df2(sanitize(params.df2JoinColumn)))
    }*/
    //params.df1.join(params.df2, params.df1(sanitize(params.df1JoinColumn)) === params.df2(sanitize(params.df2JoinColumn)))
    doJoinDataframes(params.df1, params.df2, params.df1(sanitize(params.df1JoinColumn)) === params.df2(sanitize(params.df2JoinColumn)))
    //df1
  }

  override def joinAllDataframes(params: joinAllDataframesParams): DataFrame =
    doJoinDataframes(params.df1, params.df2, lit(true))
}
