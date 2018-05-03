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
    val q = AppConfig.getString(sparqlQuerySource)
    val sparqlQuery = if (q.startsWith("file://")) {
      val filename = q.substring(7)
      Source.fromFile(filename).getLines.mkString(" ")
    }
    else {
      q
    }
    println(sparqlQuery)
    val logicalPlan = MyOpVisitorBase.newMyOpVisitorBase(sparqlQuery).getBop
    executeTree(logicalPlan(0)).get
  }

  private def getEncodedStr(decodedColumnName: String): String = {
    println(decodedColumnName)
    val enc = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(decodedColumnName)).getOrElse(0).toString
    println(enc)
    enc
  }

  private def getEncodedLong(decodedValue: String): Long = PhysicalPlanner.pointSearchKey(pointSearchKeyParams(decodedValue)).getOrElse(0)

  private def getPredicateList(node: BaseOperator): Array[String] = {
    if (node.isInstanceOf[JoinOrOperator]) {
      node.getBopChildren.asScala.foldLeft(Array[String]())((preds: Array[String], child: BaseOperator) => {
        preds ++ getPredicateList(child)
      })
    }
    else if (node.isInstanceOf[FilterOf]) {
      val filter = node.asInstanceOf[FilterOf]
      filter.getFilters.filter(column => {
        column.getColumn.getColumnTypes == ColumnTypes.PREDICATE
      }).map(column => getEncodedStr(column.getValue))
    }
    else {
      throw new Exception("Only support JoinOr and FilterOf under JoinOr")
    }
  }

  private def guessDataFrame(node: BaseOperator): Array[DataFrame] = {
    val predicates = getPredicateList(node)

    val result = DataStore.propertyData.filter(df => {
      df.hasColumns(predicates)
    })

    if (result.length == 0) {
      println("Triples")
      //Array(DataStore.triplesData)
      throw new Exception("Does not support queries on leftovers")
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

      var res = if (df.isEmpty) {
        guessDataFrame(filter)(0)
      }
      else {
        df.get
      }

      if (sub.isDefined) {
        val encodedFilter = getEncodedLong(sub.get.getValue)
        println("subject filter: " + encodedFilter)
        res = PhysicalPlanner.filterByColumn(filterByColumnParams(res, tripleSubLongField, encodedFilter))
      }

      if ((pred.isDefined) && (obj.isDefined)) {
        val encodedFilterPred = getEncodedStr(pred.get.getValue)

        //val objFilter = if (obj.get.getValue.startsWith("\"")) obj.get.getValue.substring(1, obj.get.getValue.length - 1)
        //                else obj.get.getValue
        val encodedFilterObj = getEncodedLong(obj.get.getValue)

        println("pred+obj filter: " + encodedFilterPred + " " + encodedFilterObj)

        res = PhysicalPlanner.filterByColumn(filterByColumnParams(res, encodedFilterPred, encodedFilterObj))
      }
      else if (pred.isDefined) {
        val encodedFilterPred = getEncodedStr(pred.get.getValue)

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
