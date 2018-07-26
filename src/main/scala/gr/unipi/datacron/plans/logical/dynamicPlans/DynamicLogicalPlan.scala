package gr.unipi.datacron.plans.logical.dynamicPlans

import java.text.SimpleDateFormat

import gr.unipi.datacron.common.{AppConfig, Consts, SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.MyOpVisitorBase
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.{Column, ColumnTypes}
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

case class DynamicLogicalPlan() extends BaseLogicalPlan() {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  private var logicalPlan: BaseOperator = _

  override def preparePlan(): Unit = {
    val q = AppConfig.getString(sparqlQuerySource)
    val sparqlQuery = if (q.startsWith("file://")) {
      val filename = q.substring(7)
      Source.fromFile(filename).getLines.mkString(" ")
    }
    else {
      q
    }
    //println(sparqlQuery)
    logicalPlan = MyOpVisitorBase.newMyOpVisitorBase(sparqlQuery).getBop()(0)
  }

  override def doAfterPrepare(): Unit = println(logicalPlan.getBopChildren.get(0))

  override private[logical] def doExecutePlan(): DataFrame = {
    executeTree(logicalPlan).get
  }

  private[dynamicPlans] val constraints = Try(SpatioTemporalRange(
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), AppConfig.getOptionalDouble(qfpAltLower), dateFormat.parse(AppConfig.getString(qfpTimeLower)).getTime),
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), AppConfig.getOptionalDouble(qfpAltUpper), dateFormat.parse(AppConfig.getString(qfpTimeUpper)).getTime))).toOption

  private def filterBySpatioTemporalInfo(dfO: Option[DataFrame], logicalOperator: BaseOperator): Option[DataFrame] = {
    if (dfO.isDefined && constraints.isDefined && AppConfig.getOptionalBoolean(qfpEnableFilterByEncodedInfo).getOrElse(true) && dfO.get.hasSpatialAndTemporalShortcutCols) {
      println("Filtering by spatio-temporal info")
      Option(PhysicalPlanner.filterBySubSpatioTemporalInfo(filterBySubSpatioTemporalInfoParams(dfO.get, constraints.get, encoder, Option(logicalOperator))))
    }
    else {
      dfO
    }
  }

  private def refineBySpatioTemporal(dfO: Option[DataFrame]): Option[DataFrame] = {
    if (dfO.isDefined && constraints.isDefined && dfO.get.hasSpatialAndTemporalShortcutCols()) {
      println("Filtering by spatio-temporal predicates")
      val spatialShortcutCol = dfO.get.getSpatioTemporalShortucutCols()
      val spatialShortcutCol_trans = spatialShortcutCol.map(_ + tripleTranslateSuffix)
      val df = PhysicalPlanner.decodeColumns(decodeColumnsParams(dfO.get, spatialShortcutCol))

      Option(PhysicalPlanner.filterBySpatioTemporalRange(filterBySpatioTemporalRangeParams(df, constraints.get, spatialShortcutCol_trans)))
    }
    else {
      dfO
    }
  }

  private val prefixMappings: mutable.HashMap[String, String] = mutable.HashMap[String, String]()

  private def getEncodedStr(decodedColumnName: String): String = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(decodedColumnName)).getOrElse(0).toString

  private def getEncodedLong(decodedValue: String): Long = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(decodedValue)).getOrElse(0)

  private def getDecodedStr(encodedValue: Long): String = PhysicalPlanner.decodeSingleKey(decodeSingleKeyParams(encodedValue)).getOrElse("")

  private def getPrefix(s: String): String = s.substring(0, s.indexOf('.') + 1)
  private def getSuffix(s: String): String = s.substring(s.indexOf('.') + 1)

  private def getNewJoinOrOperator(node: JoinOrOperator, preds: Array[String]): JoinOrOperator = {
    val filters = node.getBopChildren.asScala.toArray.filter(op => {
      preds.contains(op.asInstanceOf[FilterOf].getPredicate)
    })
    JoinOrOperator.newJoinOrOperator(filters: _*)
  }

  private def convertJoinOr(joinOrOperator: JoinOrOperator, incl: Array[String], excl: Array[String]): JoinOperator = {
    val joinOr1 = getNewJoinOrOperator(joinOrOperator, incl)
    val joinOr2 = getNewJoinOrOperator(joinOrOperator, excl)

    JoinOperator.newJoinOperator(joinOr1, joinOr2)
  }

  private def getPredicateList(node: BaseOperator): Array[String] = {
    node match {
      case _: JoinOrOperator =>
        node.getBopChildren.asScala.foldLeft(Array[String]())((preds: Array[String], child: BaseOperator) => {
          preds ++ getPredicateList(child)
        })
      case filter: FilterOf =>
        filter.getFilters.filter(column => {
          column.getColumn.getColumnTypes == PREDICATE
        }).map(column => getEncodedStr(column.getValue))
      case _ => throw new Exception("Only support JoinOr and FilterOfLogicalOperator under JoinOr")
    }
  }

  private def findObjectOfRdfType(node: BaseOperator): Option[String] = {
    node match {
      case fNode: FilterOf =>
        val objFilter = fNode.getFilters.find(_.getColumn.getColumnTypes == OBJECT)
        if (objFilter.isDefined) {
          Some(getEncodedStr(objFilter.get.getValue))
        }
        else None
      case jNode: JoinOrOperator =>
        jNode.getBopChildren.asScala.foreach(child => {
          val res = findObjectOfRdfType(child)
          if (res.isDefined) return res
        })
        None
    }
  }

  private def guessDataFrame(dfO: Option[DataFrame], node: BaseOperator): Array[DataFrame] = {
    if (dfO.isEmpty) {
      val rdfTypeEnc = getEncodedStr(rdfType)
      var predicates = getPredicateList(node)

      if ((predicates.length == 1) && (predicates(0) == rdfTypeEnc)) {
        if (node.isInstanceOf[FilterOf]) {
          val objFilter = node.asInstanceOf[FilterOf].getFilters.find(_.getColumn.getColumnTypes == OBJECT)
          if (objFilter.isDefined) {
            val encodedObjFilter = getEncodedStr(objFilter.get.getValue)
            return DataStore.findDataframeBasedOnRdfType(encodedObjFilter)
          }
        }
        DataStore.propertyData.filter(df => {
          df.getIncludingColumns(predicates).length > 0
        }) :+ DataStore.triplesData
      }
      else {
        if (predicates.contains(rdfTypeEnc)) {
          val objFilter = findObjectOfRdfType(node)
          if (objFilter.isDefined) {
            val dfA = DataStore.findDataframeBasedOnRdfType(objFilter.get)
            if (!dfA.contains(DataStore.nodeData)) {
              return dfA
            }
          }
        }
        predicates = predicates.filter(!_.equals(rdfTypeEnc))

        val result = DataStore.propertyData.filter(df => {
          df.getIncludingColumns(predicates).length > 0
        })

        if (result.length == 0) {
          Array(DataStore.triplesData)
        }
        else if (result.length == 1) {
          val df = result(0)
          var excl = df.getExcludingColumns(predicates)
          if (excl.length > 0) {
            Array(df, DataStore.triplesData)
          }
          else {
            Array(df)
          }
        }
        else {
          throw new Exception("Does not support more than one dataframes")
        }
      }
    }
    else {
      Array(dfO.get)
    }
  }

  private def getIncludingAndExcludingCols(propertyDf: DataFrame, node: BaseOperator): (Array[String], Array[String]) = {
    val predicates = getPredicateList(node)

    val incl = propertyDf.getIncludingColumns(predicates).map(_.toLong).map(getDecodedStr)
    val excl = propertyDf.getExcludingColumns(predicates).map(_.toLong).map(getDecodedStr)

    (incl, excl)
  }

  private def processFilterOf(filter: FilterOf, dfO: Option[DataFrame]) : Option[DataFrame] = {
    val sub = filter.getFilters.find(_.getColumn.getColumnTypes == ColumnTypes.SUBJECT)
    val pred = filter.getFilters.find(_.getColumn.getColumnTypes == PREDICATE)
    val obj = filter.getFilters.find(_.getColumn.getColumnTypes == ColumnTypes.OBJECT)

    if (pred.isEmpty) {
      throw new Exception("A predicate filter should be provided!")
    }

    //filter.getFilters.foreach(println)

    val dfs = guessDataFrame(dfO, filter)

    val encodedFilterPred = getEncodedStr(pred.get.getValue)

    if (dfs.length > 1) {
      var dfHead = processFilterOf(filter, Option(dfs.head)).get
      dfHead = PhysicalPlanner.selectColumns(selectColumnsParams(dfHead, Array(tripleSubLongField, encodedFilterPred)))

      Option(dfs.tail.foldLeft(dfHead)((df1, df2) => {
        var df = processFilterOf(filter, Option(df2)).get
        df = PhysicalPlanner.selectColumns(selectColumnsParams(df, Array(tripleSubLongField, encodedFilterPred)))
        PhysicalPlanner.unionDataframes(unionDataframesParams(df1, df, Option(filter)))
      }))
    }
    else {
      var df = dfs(0)

      if (sub.isDefined) {
        val encodedFilter = getEncodedLong(sub.get.getValue)
        df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, tripleSubLongField, encodedFilter, Option(filter)))
      }

      if (df.isPropertyTable) {
        if (obj.isDefined) {
          val encodedFilterObj = getEncodedLong(obj.get.getValue)
          df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, encodedFilterPred, encodedFilterObj, Option(filter)))
        }
        else {
          df = PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(df, Array(encodedFilterPred), Option(filter)))
        }
      }
      else {
        df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, triplePredLongField, encodedFilterPred, Option(filter)))
        if (obj.isDefined) {
          val encodedFilterObj = getEncodedLong(obj.get.getValue)
          df = PhysicalPlanner.filterByColumn(filterByColumnParams(df, tripleObjLongField, encodedFilterObj, Option(filter)))
        }
        df = PhysicalPlanner.dropColumns(dropColumnsParams(df, Array(triplePredLongField)))
        df = PhysicalPlanner.renameColumns(renameColumnsParams(df, Map((tripleObjLongField, encodedFilterPred))))
      }

      //df.show()
      Option(df)
    }
  }

  private def processJoinOr(joinOr: JoinOrOperator, dfO: Option[DataFrame]) : Option[DataFrame] = {
    val dfs = guessDataFrame(dfO, joinOr)

    if (dfs.length > 1) {
      val (incl, excl) = getIncludingAndExcludingCols(dfs(0), joinOr)
      val join = convertJoinOr(joinOr, incl, excl)
      val res = recursivelyExecuteNode(join, None)
      joinOr.setRealOutputSize(join.getRealOutputSize)
      res
    }
    else {
      val dfO = Option(dfs(0))
      if (dfO.get.isPropertyTable) {
        val result = joinOr.getBopChildren.asScala.foldLeft(dfO)((dfTmp: Option[DataFrame], child: BaseOperator) => {
          recursivelyExecuteNode(child, dfTmp)
        })
        joinOr.setRealOutputSize(joinOr.getBopChildren.asScala.last.getRealOutputSize)
        val dfO1 = filterBySpatioTemporalInfo(result, joinOr)
        if (AppConfig.getBoolean(qfpEnableRefinementPushdown)) {
          refineBySpatioTemporal(dfO1)
        }
        else {
          dfO1
        }
      }
      else {
        val df = if (AppConfig.getBoolean(qfpEnableMultipleFilterJoinOr)) {
          val filters = getPredicateList(joinOr).map((triplePredLongField, _))
          Option(PhysicalPlanner.filterByMultipleOr(filterByMultipleOrParams(dfO.get, filters, Option(joinOr))).cache)
        } else {
          dfO
        }

        executeJoin(joinOr, joinOr.getColumnJoinPredicate, df)
      }
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
    //println("joinChild: " + op.getClass)
    val prefix1 = getPrefixForColumn(df1, op, col)
    val columnName1 = getColumnNameForJoin(op, col, prefix1._1)

    (prefix1._2, columnName1)
  }

  private def executeJoin(joinOp: BaseOperator, joinCols: Array[Column], df: Option[DataFrame]): Option[DataFrame] = {
    /*val children = joinOp.getBopChildren.asScala.zipWithIndex
    val dfAndCol1 = getDfAndColNameForJoin(children.head._1, joinCols(0), df)

    Option(children.tail.foldLeft(dfAndCol1)((dfAndCol1: (DataFrame, String), child: (BaseOperator, Int)) => {
      val dfAndCol2 = getDfAndColNameForJoin(child._1, joinCols(child._2), df)
      (PhysicalPlanner.joinDataframes(joinDataframesParams(dfAndCol1._1, dfAndCol2._1, dfAndCol1._2, dfAndCol2._2, Option(joinOp))), dfAndCol1._2)
    })._1)*/

    if (joinOp.getBopChildren.size() > 2) {
      throw new Exception("Not supported join between more than 2 operators")
    }
    else if (joinOp.getBopChildren.size() == 1) {
      val child1 = joinOp.getBopChildren.get(0)
      Option(getDfAndColNameForJoin(child1, joinCols(0), df)._1)
    }
    else {
      val child1 = joinOp.getBopChildren.get(0)
      val child2 = joinOp.getBopChildren.get(1)
      val dfAndCol1 = getDfAndColNameForJoin(child1, joinCols(0), df)
      val dfAndCol2 = getDfAndColNameForJoin(child2, joinCols(1), df)
      val child1Size = if (child1.getOutputSize >= 0) child1.getOutputSize else Long.MaxValue
      val child2Size = if (child2.getOutputSize >= 0) child2.getOutputSize else Long.MaxValue
      Option(PhysicalPlanner.joinDataframes(joinDataframesParams(dfAndCol1._1, dfAndCol2._1, dfAndCol1._2, dfAndCol2._2, child1Size, child2Size, Option(joinOp))))
    }
  }

  private def processJoin(joinOp: JoinOperator) : Option[DataFrame] = executeJoin(joinOp, joinOp.getColumnJoinPredicate, None)

  private def processSelect(selectOp: SelectOperator) : Option[DataFrame] = {
    val dfO = recursivelyExecuteNode(selectOp.getBopChildren.get(0), None)
    if (dfO.isDefined) {
      projectResults(filterFinalResults(dfO), selectOp)
    }
    else None
  }

  private def recursivelyExecuteNode(node: BaseOperator, df: Option[DataFrame]): Option[DataFrame] = {
    node match {
      case f: FilterOf => processFilterOf(f, df)
      case jo: JoinOrOperator => processJoinOr(jo, df)
      case j: JoinOperator => processJoin(j)
      case s: SelectOperator => processSelect(s)
      case _ => None
    }
  }

  private def filterFinalResults(dfO: Option[DataFrame]): Option[DataFrame] = {
    //Perform refinement based on Filter operators here...
    if (!AppConfig.getBoolean(qfpEnableRefinementPushdown)) {
      refineBySpatioTemporal(dfO)
    }
    else {
      dfO
    }
  }

  private def projectResults(dfO: Option[DataFrame], s: SelectOperator): Option[DataFrame] = {
    if (dfO.isEmpty) {
      None
    }
    else {
      val df = dfO.get
      val child: BaseOperator = s.getBopChildren.get(0)
      val findColPred = (colName: String) => {
        child.getArrayColumns.find(c => c.getColumnName.equals(colName)).get
      }

      val vars = if (s.getVariables.size() == 0) {
        child.getArrayColumns.filter(c => c.getQueryString.startsWith("?") && !c.getQueryString.startsWith("??")).map(_.getQueryString)
      }
      else s.getVariables.asScala.toArray

      val cols = vars.map(v => {
        val col = child.getArrayColumns.find(c => c.getQueryString.equals(v)).get
        val pref = getPrefix(col.getColumnName)
        val mPref = prefixMappings.getOrElse(pref, "")
        //val mPref = mPrefO.getOrElse("")

        col.getColumnTypes match {
          case ColumnTypes.SUBJECT =>
            val tmp = mPref + tripleSubLongField
            df(sanitize(tmp)).as(col.getQueryString)
          case ColumnTypes.OBJECT =>
            val colP = findColPred(pref + "Predicate")
            val enc = getEncodedStr(colP.getQueryString)
            df(sanitize(mPref + enc)).as(col.getQueryString)
        }
      })
      //cols.foreach(println)
      val newDf = df.select(cols: _*)

      if (AppConfig.getOptionalBoolean(qfpEnableResultDecode).getOrElse(true)) {
        Option(PhysicalPlanner.decodeColumns(decodeColumnsParams(newDf, newDf.columns, preserveColumnNames = true)))
      }
      else Option(newDf)
    }
  }

  private def executeTree(root: BaseOperator): Option[DataFrame] = {
    recursivelyExecuteNode(root, None)
  }
}
