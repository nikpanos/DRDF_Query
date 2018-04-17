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
import gr.unipi.datacron.plans.physical.traits.{filterByColumnParams, filterNullPropertiesParams, pointSearchKeyParams}
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

  private def getEncodedColumnName(decodedColumnName: String): String = {
    println(decodedColumnName)
    val enc = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(decodedColumnName)).getOrElse(0).toString
    println(enc)
    enc
  }

  private def getEncodedValue(decodedValue: String): Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(decodedValue)).getOrElse(0)

  private def getPredicateList(node: JoinOrOperator): Array[String] = {
    node.getBopChildren.asScala.foldLeft(Array[String]())((preds: Array[String], child: BaseOperator) => {
      if (child.isInstanceOf[JoinOrOperator]) preds ++ getPredicateList(child.asInstanceOf[JoinOrOperator])
      else if (child.isInstanceOf[FilterOf]) {
        val filter = child.asInstanceOf[FilterOf]
        val column = filter.getFilters.find(column => {
          column.getColumn.getColumnTypes == ColumnTypes.PREDICATE
        })
        if (column.isDefined) {
          preds :+ getEncodedColumnName(column.get.getValue)
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
      println("Triples")
      Array(DataStore.triplesData)
    }
    else if (result.length == 1) {
      println("one")
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
        val encodedFilter = getEncodedValue(sub.get.getValue)
        println("subject filter: " + encodedFilter)
        res = PhysicalPlanner.filterByColumn(filterByColumnParams(res, tripleSubLongField, encodedFilter))
      }

      if ((pred.isDefined) && (obj.isDefined)) {
        val encodedFilterPred = getEncodedColumnName(pred.get.getValue)
        val encodedFilterObj = getEncodedValue(obj.get.getValue)

        println("pred+obj filter: " + encodedFilterPred + " " + encodedFilterObj)

        res = PhysicalPlanner.filterByColumn(filterByColumnParams(res, encodedFilterPred, encodedFilterObj))
      }
      else if (pred.isDefined) {
        val encodedFilterPred = getEncodedColumnName(pred.get.getValue)

        println("pred filter: " + encodedFilterPred)

        res = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(res, Array(encodedFilterPred)))
      }
      else if ((obj.isDefined) && (pred.isEmpty)) {
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
