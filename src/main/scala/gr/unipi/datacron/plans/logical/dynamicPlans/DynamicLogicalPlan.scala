package gr.unipi.datacron.plans.logical.dynamicPlans

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

import scala.io.Source

case class DynamicLogicalPlan() extends BaseLogicalPlan() {
  override private[logical] def doExecutePlan() : DataFrame = DataStore.nodeData

  def myPlanExecution(): Unit = {
    val sparqlQuery = Source.fromFile(AppConfig.getString(sparqlQuerySource)).getLines.mkString(" ")
    println(sparqlQuery)
    val logicalPlan = MyOpVisitorBase.newMyOpVisitorBase(sparqlQuery).getBop
    println(logicalPlan.length)
  }
}
