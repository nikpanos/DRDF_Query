package gr.unipi.datacron.plans.logical.dynamicPlans

import gr.unipi.datacron.common.{AppConfig, Consts}
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.{Column, ColumnTypes}
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.{BaseOperator, FilterOf, JoinOperator, JoinOrOperator}
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._
import scala.collection.mutable
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

  private val prefixMappings: mutable.HashMap[String, String] = mutable.HashMap[String, String]()

  private def getEncodedStr(decodedColumnName: String): String = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(decodedColumnName)).getOrElse(0).toString

  private def getEncodedLong(decodedValue: String): Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(decodedValue)).getOrElse(0)

  private def getDecodedStr(encodedValue: Long): String = PhysicalPlanner.decodeSingleKey(decodeSingleKeyParams(encodedValue)).getOrElse("")

  private def getPrefix(s: String): String = s.substring(0, s.indexOf('.') + 1)
  private def getSuffix(s: String): String = s.substring(s.indexOf('.') + 1)

  private def getPredicateList(node: BaseOperator): Array[String] = {
    node match {
      case _: JoinOrOperator =>
        node.getBopChildren.asScala.foldLeft(Array[String]())((preds: Array[String], child: BaseOperator) => {
          preds ++ getPredicateList(child)
        })
      case _: FilterOf =>
        val filter = node.asInstanceOf[FilterOf]
        filter.getFilters.filter(column => {
          column.getColumn.getColumnTypes == PREDICATE
        }).map(column => getEncodedStr(column.getValue))
      case _ => throw new Exception("Only support JoinOr and FilterOf under JoinOr")
    }
  }

  private def guessDataFrame(dfO: Option[DataFrame], node: BaseOperator): DataFrame = {
    if (dfO.isEmpty) {
      val predicates = getPredicateList(node)

      val result = DataStore.propertyData.filter(df => {
        df.hasColumns(predicates)
      })

      if (result.length == 0) {
        DataStore.triplesData
      }
      else if (result.length == 1) {
        result(0)
      }
      else {
        throw new Exception("Does not support more than one dataframes")
      }
    }
    else {
      dfO.get
    }
  }

  private def processFilterOf(filter: FilterOf, dfO: Option[DataFrame]) : Option[DataFrame] = {
    val sub = filter.getFilters.find(_.getColumn.getColumnTypes == ColumnTypes.SUBJECT)
    val pred = filter.getFilters.find(_.getColumn.getColumnTypes == PREDICATE)
    val obj = filter.getFilters.find(_.getColumn.getColumnTypes == ColumnTypes.OBJECT)

    if (pred.isEmpty) {
      throw new Exception("A predicate filter should be provided!")
    }

    var df = guessDataFrame(dfO, filter)

    if (sub.isDefined) {
      val encodedFilter = getEncodedLong(sub.get.getValue)
      df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, tripleSubLongField, encodedFilter))
    }

    val encodedFilterPred = getEncodedStr(pred.get.getValue)
    if (df.isPropertyTable) {
      if (obj.isDefined) {
        val encodedFilterObj = getEncodedLong(obj.get.getValue)
        df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, encodedFilterPred, encodedFilterObj))
      }
      else {
        df = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(df, Array(encodedFilterPred)))
      }
    }
    else {
      df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, triplePredLongField, encodedFilterPred))
      if (obj.isDefined) {
        val encodedFilterObj = getEncodedLong(obj.get.getValue)
        df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, tripleObjLongField, encodedFilterObj))
      }
      df = PhysicalPlanner.dropColumns(dropColumnsParams(df, Array(triplePredLongField)))
      df = PhysicalPlanner.renameColumns(renameColumnsParams(df, Map((tripleObjLongField, encodedFilterPred))))
    }

    Option(df)
  }

  private def processJoinOr(joinOr: JoinOrOperator, dfO: Option[DataFrame]) : Option[DataFrame] = {
    val df = Option(guessDataFrame(dfO, joinOr))

    if (df.isDefined && df.get.isPropertyTable) {
      joinOr.getBopChildren.asScala.foldLeft(df)((dfTmp: Option[DataFrame], child: BaseOperator) => {
        recursivelyExecuteNode(child, dfTmp)
      })
    }
    else if (df.isDefined) {
      executeJoin(joinOr, joinOr.getColumnJoinPredicate, df)
    }
    else {
      None
    }
  }

  private def getColumnNameForJoin(op: BaseOperator, c: Column, prefix: String): String = {
    prefix + (c.getColumnTypes match {
      case ColumnTypes.SUBJECT => Consts.tripleSubLongField
      case PREDICATE => throw new Exception("Does not support join predicates on Predicate columns")
      case _ =>
        val fil = c.getColumnName.substring(0, c.getColumnName.indexOf('.')) + ".Predicate"
        val colName = op.getArrayColumns.find(c => c.getColumnName.equals(fil)).get.getQueryString
        getEncodedStr(colName)
    })
  }

  private def getPrefixForColumn(df: DataFrame, op: BaseOperator, col: Column): (String, DataFrame) = {
    val pref = getPrefix(col.getColumnName)
    if (!df.isPrefixed) {
      op.getArrayColumns.foreach(c => prefixMappings.put(getPrefix(c.getColumnName), pref))
      (pref, PhysicalPlanner.prefixColumns(prefixColumnsParams(df, pref)))
    }
    else {
      (prefixMappings(pref), df)
    }
  }

  private def getDfAndColNameForJoin(op: BaseOperator, col: Column, dfO: Option[DataFrame]): (DataFrame, String) = {
    val df1 = recursivelyExecuteNode(op, dfO).get
    val prefix1 = getPrefixForColumn(df1, op, col)
    val columnName1 = getColumnNameForJoin(op, col, prefix1._1)

    (prefix1._2, columnName1)
  }

  private def executeJoin(joinOp: BaseOperator, joinCols: Array[Column], df: Option[DataFrame]): Option[DataFrame] = {
    val children = joinOp.getBopChildren.asScala.zipWithIndex
    val dfAndCol1 = getDfAndColNameForJoin(children.head._1, joinCols(0), df)

    Option(children.tail.foldLeft(dfAndCol1)((dfAndCol1: (DataFrame, String), child: (BaseOperator, Int)) => {
      val dfAndCol2 = getDfAndColNameForJoin(child._1, joinCols(child._2), df)
      (PhysicalPlanner.joinDataframes(joinDataframesParams(dfAndCol1._1, dfAndCol2._1, dfAndCol1._2, dfAndCol2._2)), dfAndCol1._2)
    })._1)
  }

  private def processJoin(joinOp: JoinOperator) : Option[DataFrame] = executeJoin(joinOp, joinOp.getColumnJoinPredicate, None)

  private def recursivelyExecuteNode(node: BaseOperator, df: Option[DataFrame]): Option[DataFrame] = {
    node match {
      case f: FilterOf => processFilterOf(f, df)
      case jo: JoinOrOperator => processJoinOr(jo, df)
      case j: JoinOperator => processJoin(j)
      case _ => None
    }
  }

  private def projectResults(dfO: Option[DataFrame], root: BaseOperator): Option[DataFrame] = {
    if (dfO.isEmpty) {
      None
    }
    else {
      val df = dfO.get
      val subjects = mutable.HashMap[String, String]()
      val cols = root.getArrayColumns.filter(_.getColumnTypes != ColumnTypes.OBJECT).map(c => {
        val pref = getPrefix(c.getColumnName)
        val mPrefO = prefixMappings.get(pref)

        val mPref = mPrefO.getOrElse("")

        val colName = if (c.getColumnTypes == ColumnTypes.SUBJECT) {
          val tmp = mPref + tripleSubLongField
          subjects.put(tmp, c.getQueryString)
          sanitize(tmp)
        }
        else {
          val enc = getEncodedStr(c.getQueryString)
          sanitize(mPref + enc)
        }
        if (df.hasColumn(colName + tripleTranslateSuffix))
          colName + tripleTranslateSuffix
        else
          colName
      })
      val newDf = df.select(cols.head, cols.tail: _*)

      val newDf1 = PhysicalPlanner.decodeColumns(decodeColumnsParams(newDf, newDf.columns.filter(!_.endsWith(tripleTranslateSuffix)), preserveColumnNames = true))

      val renamedColumns = newDf1.columns.map(c => {
        val res = subjects.get(c)
        val newColName = if (res.isDefined) {
          res.get
        }
        else {
          getDecodedStr(getSuffix(c).toLong)
        }
        newDf1(sanitize(c)).as(s"$newColName")
      })
      Option(newDf1.select(renamedColumns: _*))
    }
  }

  private def executeTree(root: BaseOperator): Option[DataFrame] = {
    projectResults(recursivelyExecuteNode(root, None), root)
  }
}
