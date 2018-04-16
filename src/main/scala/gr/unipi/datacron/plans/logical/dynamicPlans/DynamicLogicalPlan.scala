package gr.unipi.datacron.plans.logical.dynamicPlans

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.{BaseOperator, FilterOf, JoinOrOperator}
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.{filterByColumnParams, filterNullPropertiesParams}
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._
import scala.io.Source

case class DynamicLogicalPlan() extends BaseLogicalPlan() {
  override private[logical] def doExecutePlan(): DataFrame = {
    val sparqlQuery = Source.fromFile(AppConfig.getString(sparqlQuerySource)).getLines.mkString(" ")
    println(sparqlQuery)
    val logicalPlan = MyOpVisitorBase.newMyOpVisitorBase(sparqlQuery).getBop
    executeTree(logicalPlan(0)).get
  }

  private def getPredicateList(node: JoinOrOperator): Array[String] = {
    node.getBopChildren.asScala.foldLeft(Array[String]())((preds: Array[String], child: BaseOperator) => {
      if (child.isInstanceOf[JoinOrOperator]) preds ++ getPredicateList(child.asInstanceOf[JoinOrOperator])
      else if (child.isInstanceOf[FilterOf]) {
        val filter = child.asInstanceOf[FilterOf]
        val column = filter.getFilters.find(column => {
          column.getColumn.getColumnTypes == ColumnTypes.PREDICATE
        })
        if (column.isDefined) {
          preds :+ column.get.getValue
        }
        else {
          preds
        }
      }
      else {
        throw new Exception("Only support JoinOr and FilterOf under JoinOr")
      }
    })
  }

  private def guessDataFrame(node: JoinOrOperator): Array[DataFrame] = {
    val predicates = getPredicateList(node)

    val result = DataStore.propertyData.filter(df => {
      df.hasColumns(predicates)
    })

    if (result.length == 0) {
      Array(DataStore.triplesData)
    }
    else if (result.length == 1) {
      result
    }
    else {
      throw new Exception("Does not support more than one dataframes")
    }
  }

  private def recursivelyExecuteNode(node: BaseOperator, df: Option[DataFrame]): Option[DataFrame] = {
    if (node.isInstanceOf[FilterOf]) {
      val filter = node.asInstanceOf[FilterOf]
      val sub = filter.getFilters.find(_.getColumn.getColumnTypes == ColumnTypes.SUBJECT)
      val pred = filter.getFilters.find(_.getColumn.getColumnTypes == ColumnTypes.PREDICATE)
      val obj = filter.getFilters.find(_.getColumn.getColumnTypes == ColumnTypes.OBJECT)

      var res = df.get

      if (sub.isDefined) {
        res = PhysicalPlanner.filterByColumn(filterByColumnParams(res, tripleSubLongField, sub.get.getValue))
      }

      if ((pred.isDefined) && (obj.isDefined)) {
        res = PhysicalPlanner.filterByColumn(filterByColumnParams(res, pred.get.getValue, obj.get.getValue))
      }
      else if (pred.isDefined) {
        res = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(res, Array(pred.get.getValue)))
      }
      else {
        throw new Exception("Filter on object without filter on predicate is not supported!")
      }

      Option(res)
    }
    else if (node.isInstanceOf[JoinOrOperator]) {
      val joinOr = node.asInstanceOf[JoinOrOperator]

      val df1 = if (df.isEmpty) {
        Option(guessDataFrame(joinOr)(0))
      }
      else {
        df
      }

      joinOr.getBopChildren.asScala.foldLeft(df1)((dfTmp: Option[DataFrame], child: BaseOperator) => {
        recursivelyExecuteNode(child, dfTmp)
      })
    }
    else {
      None
    }
  }

  private def executeTree(root: BaseOperator): Option[DataFrame] = {
    recursivelyExecuteNode(root, None)
  }
}
