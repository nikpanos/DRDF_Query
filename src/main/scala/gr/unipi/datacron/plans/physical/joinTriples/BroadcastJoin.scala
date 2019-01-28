package gr.unipi.datacron.plans.physical.joinTriples

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.Utils._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.{TJoinTriples, joinAllDataframesParams, joinDataframesParams}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{broadcast, lit}

case class BroadcastJoin() extends BasePhysicalPlan with TJoinTriples {

  private def doJoinDataframes(leftDf: DataFrame, rightDf: DataFrame, col: Column, leftEstimatedSize: Long, rightEstimatedSize: Long): DataFrame = {
    val thres = AppConfig.getOptionalLong(qfpBroadcastThreshold)
    if (thres.isDefined && (leftEstimatedSize <= thres.get)) {
      //println("Forcing broadcast join on df1: " + params.df1EstimatedSize)
      rightDf.join(broadcast(leftDf), col)
    }
    else if (thres.isDefined && (rightEstimatedSize <= thres.get)) {
      //println("Forcing broadcast join on df2: " + params.df2EstimatedSize)
      leftDf.join(broadcast(rightDf), col)
      //doJoinDataframes(params.df1, params.df2, params.df1(sanitize(params.df1JoinColumn)) === params.df2(sanitize(params.df2JoinColumn)))
    }
    else {
      //println("No forced broadcast join")
      leftDf.join(rightDf, col)
      //params.df1.join(params.df2, params.df1(sanitize(params.df1JoinColumn)) === params.df2(sanitize(params.df2JoinColumn)))
    }
  }


  override def joinDataframes(params: joinDataframesParams): DataFrame =
    doJoinDataframes(params.df1, params.df2, params.df1(params.df1JoinColumn) === params.df2(params.df2JoinColumn), params.df1EstimatedSize, params.df2EstimatedSize)

  override def joinAllDataframes(params: joinAllDataframesParams): DataFrame =
    doJoinDataframes(params.df1, params.df2, lit(true), params.df1EstimatedSize, params.df2EstimatedSize)
}
