package gr.unipi.datacron.plans.logical.dynamicPlans

import java.text.SimpleDateFormat

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.encodeSingleValueParams
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

import collection.JavaConverters._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.common.{AppConfig, SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.dataOperators.{DatasourceOperator, SortEnums}
import gr.unipi.datacron.plans.logical.dynamicPlans.analyzedOperators.logicalOperators.{BooleanTrait, ConditionOperator, LogicalAggregateEnums}
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.{ColumnTypes, ConditionType, OperandPair}
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.{BaseOperand, ColumnOperand, ValueOperand}

import scala.util.Try


class PlanAnalyzer() {

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  private var shouldApplyExactSpatioTemporalFilterLater = false

  private val constraints = Try(SpatioTemporalRange(
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), AppConfig.getOptionalDouble(qfpAltLower), dateFormat.parse(AppConfig.getString(qfpTimeLower)).getTime),
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), AppConfig.getOptionalDouble(qfpAltUpper), dateFormat.parse(AppConfig.getString(qfpTimeUpper)).getTime))).toOption

  private def getChildren(bop: BaseOperator): Array[BaseOperator] = bop.getBopChildren

  private def getEncodedStr(decodedColumnName: String): String = {
    val result = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(decodedColumnName))
    if (result.isEmpty){
      throw new Exception("Could not find encoded value for column name: " + decodedColumnName)
    }
    else {
      result.get.toString
    }
  }

  private def getSelectOperators(so: SelectOperator, columnType: ColumnTypes): Array[OperandPair] = {
    so.getFilters.filter(operandPair => {
      operandPair.getLeftOperand match {
        case column: ColumnOperand =>
          column.getColumn.getColumnTypes == columnType
        case _ => throw new Exception("Should be ColumnOperand")
      }
    })
  }

  private def findSelectOperator(so: SelectOperator, columnType: ColumnTypes): Option[OperandPair] = {
    so.getFilters.find(operandPair => {
      operandPair.getLeftOperand match {
        case column: ColumnOperand =>
          column.getColumn.getColumnTypes == columnType
        case _ => throw new Exception("Should be ColumnOperand")
      }
    })
  }

  private def getPredicateList(node: BaseOperator): Array[String] = {
    node match {
      case _: JoinSubjectOperator =>
        getChildren(node).foldLeft(Array[String]())((preds: Array[String], child: BaseOperator) => {
          preds ++ getPredicateList(child)
        })
      case so: SelectOperator =>
        getSelectOperators(so, PREDICATE).map(operandPair => getEncodedStr(operandPair.getRightOperand.asInstanceOf[ValueOperand].getValue))
      case _ => throw new Exception("Only support JoinOr and FilterOfLogicalOperator under JoinOr")
    }
  }

  private def findObjectOfRdfType(node: BaseOperator): Option[String] = {
    node match {
      case sNode: SelectOperator =>
        val objFilter = findSelectOperator(sNode, OBJECT)
        if (objFilter.isDefined) {
          Some(getEncodedStr(objFilter.get.getRightOperand.asInstanceOf[ValueOperand].getValue))
        }
        else None
      case jsNode: JoinSubjectOperator =>
        getChildren(jsNode).foreach(child => {
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
        node match {
          case so: SelectOperator =>
            val objFilter = findSelectOperator(so, OBJECT)
            if (objFilter.isDefined) {
              val encodedObjFilter = getEncodedStr(objFilter.get.getRightOperand.asInstanceOf[ValueOperand].getValue)
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
          val excl = df.getExcludingColumns(predicates)
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

  private def findFirstDatasourceOperator(bo: analyzedOperators.commonOperators.BaseOperator): DatasourceOperator = {
    bo match {
      case ds: DatasourceOperator => ds
      case _=> findFirstDatasourceOperator(bo)
    }
  }


  private def convertSelectToUnionOperator(so: SelectOperator, dfs: Array[DataFrame]): analyzedOperators.dataOperators.UnionOperator = {
    val selOps = dfs.map(df => {
      val bo = processNode(so, Some(df))
      val ds = findFirstDatasourceOperator(bo)
      if (ds.isPropertyTableSource) {
        val colName = getEncodedStr(findSelectOperator(so, PREDICATE).get.getRightOperand.asInstanceOf[ValueOperand].getValue)
        analyzedOperators.columnOperators.ProjectOperator(bo, Array(tripleSubLongField, colName))
      }
      else {
        bo
      }
    })
    analyzedOperators.dataOperators.UnionOperator(selOps)
  }

  private def refineBySpatioTemporalInfo(child: analyzedOperators.commonOperators.BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    if (shouldApplyExactSpatioTemporalFilterLater) {
      shouldApplyExactSpatioTemporalFilterLater = false
      analyzedOperators.spatiotemporalOperators.ExactBoxOperator(child, constraints.get)
    }
    else {
      child
    }
  }

  private def getPushedDownSpatioTemporalOperator(df: DataFrame, child: analyzedOperators.commonOperators.BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    if (constraints.isDefined && df.hasSpatialAndTemporalShortcutCols) {
      val newOp = if (AppConfig.getOptionalBoolean(qfpEnableFilterByEncodedInfo).getOrElse(true)) {
        shouldApplyExactSpatioTemporalFilterLater = true
        analyzedOperators.spatiotemporalOperators.ApproximateBoxOperator(child, constraints.get)
      } else { child }
      if (AppConfig.getBoolean(qfpEnableRefinementPushdown)) {
        refineBySpatioTemporalInfo(child)
      } else { newOp }
    }
    else { child }
  }

  private def processSelectOperator(so: SelectOperator, dfO: Option[DataFrame]): analyzedOperators.commonOperators.BaseOperator = {
    so.getChild match {
      case _: TripleOperator =>
        val dfs = guessDataFrame(dfO, so)
        if (dfs.length > 1) {
          convertSelectToUnionOperator(so, dfs)
        }
        else {
          val dso = analyzedOperators.dataOperators.DatasourceOperator(dfs(0))

          val spto = getPushedDownSpatioTemporalOperator(dfs(0), dso)

          val selOp = createSelectOperator(so, spto)
          if (!dso.isPropertyTableSource) {
            val po = analyzedOperators.columnOperators.ProjectOperator(selOp, Array(tripleSubLongField, tripleObjLongField))
            val newColName = getEncodedStr(findSelectOperator(so, PREDICATE).get.getRightOperand.asInstanceOf[ValueOperand].getValue)
            analyzedOperators.columnOperators.RenameOperator(po, Array((tripleObjLongField, newColName)))
          }
          else {
            selOp
          }
        }
      case _ =>
        val child = processNode(so.getChild, dfO)
        createSelectOperator(so, child)
    }

  }

  private def getOperandStr(op: BaseOperand): String = {
    op match {
      case co: ColumnOperand => co.getColumn.getColumnName
      case vo: ValueOperand => vo.getValue
      case _ => throw new Exception("Not supported Operand!")
    }
  }

  private def getConditionOperatorFromOperandPair(operandPair: OperandPair): ConditionOperator = {
    val leftStr = getOperandStr(operandPair.getLeftOperand)
    val rightStr = getOperandStr(operandPair.getLeftOperand)
    analyzedOperators.logicalOperators.ConditionOperator(leftStr, operandPair.getConditionType, rightStr)
  }

  private def createSelectOperator(so: SelectOperator, child: analyzedOperators.commonOperators.BaseOperator): analyzedOperators.dataOperators.SelectOperator = {
    val filters = so.getFilters
    val condition = if (filters.length == 1) {
      getConditionOperatorFromOperandPair(filters(0))
    }
    else {
      val first = filters.head

      val conditionFirst: analyzedOperators.commonOperators.BaseOperator with BooleanTrait = getConditionOperatorFromOperandPair(first)
      filters.tail.foldLeft(conditionFirst)((conditionLeft, op) => {
        val conditionRight = getConditionOperatorFromOperandPair(op)
        analyzedOperators.logicalOperators.LogicalAggregateOperator(conditionLeft, conditionRight, LogicalAggregateEnums.And)
      })
    }
    analyzedOperators.dataOperators.SelectOperator(child, condition)
  }

  def processSortOperator(so: SortOperator, dfO: Option[DataFrame]): analyzedOperators.dataOperators.SortOperator = {
    val child = refineBySpatioTemporalInfo(processNode(so.getChild, dfO))
    val colWithDirections = so.getColumnWithDirection.map(cwd => {
      val sortEnum: SortEnums.SortEnum = cwd.getDirection match {
        case 1 => SortEnums.Asc
        case -1 => SortEnums.Desc
        case _ => throw new Exception("Unrecognized sorting direction")
      }
      (cwd.getColumn.getColumnName, sortEnum)
    })

    analyzedOperators.dataOperators.SortOperator(child, colWithDirections)
  }

  def processUnionOperator(uo: UnionOperator, dfO: Option[DataFrame]): analyzedOperators.dataOperators.UnionOperator = {
    val leftChild = refineBySpatioTemporalInfo(processNode(uo.getLeftChild, dfO))
    val rightChild = refineBySpatioTemporalInfo(processNode(uo.getRightChild, dfO))
    analyzedOperators.dataOperators.UnionOperator(Array(leftChild, rightChild))
  }

  def processLimitOperator(lo: LimitOperator, dfO: Option[DataFrame]): analyzedOperators.dataOperators.LimitOperator = {
    val child = refineBySpatioTemporalInfo(processNode(lo.getChild, dfO))
    analyzedOperators.dataOperators.LimitOperator(child, lo.getLimit)
  }

  def processJoinOperator(jo: JoinOperator, dfO: Option[DataFrame]): analyzedOperators.dataOperators.JoinOperator = {
    val leftChild = processNode(jo.getLeftChild, dfO)
    val rightChild = processNode(jo.getRightChild, dfO)

    val cols = jo.getColumnJoinPredicate
    val condition = if (cols.isEmpty) {
      None
    }
    else {
      Some(analyzedOperators.logicalOperators.ConditionOperator(cols(0).getColumnName, ConditionType.EQ, cols(1).getColumnName))
    }
    analyzedOperators.dataOperators.JoinOperator(leftChild, rightChild, condition)
  }

  def processJoinSubjectOperator(jso: JoinSubjectOperator, dfO: Option[DataFrame]): analyzedOperators.commonOperators.BaseOperator = {

    def processPropertyTable(selectOps: Array[SelectOperator], df: DataFrame): analyzedOperators.commonOperators.BaseOperator = {
      val firstProperty = processNode(selectOps.head, Some(df))
      selectOps.tail.foldLeft(firstProperty)((child, so) => {
        createSelectOperator(so, child)
      })
    }

    def processTriplesTable(selectOps: Array[SelectOperator], df: DataFrame): analyzedOperators.commonOperators.BaseOperator = {
      val triples = selectOps.map(x => processNode(x, Some(df)))
      val condition = Some(analyzedOperators.logicalOperators.ConditionOperator(tripleSubLongField, ConditionType.EQ, tripleSubLongField))
      val firstJoin = analyzedOperators.dataOperators.JoinOperator(triples(0), triples(1), condition)
      triples.slice(2, triples.length).foldLeft(firstJoin)((joinOp, bop) => {
        analyzedOperators.dataOperators.JoinOperator(joinOp, bop, condition)
      })
    }

    val dfs = guessDataFrame(dfO, jso)
    if (dfs.length > 1) {
      if (dfs.length > 2) {
        throw new Exception("More than 2 dataframes are not expected here")
      }
      val propertyDf = dfs.find(_.isPropertyTable).get
      val triplesDf = dfs.find(!_.isPropertyTable).get
      val (inclSo, exclSo) = getChildren(jso).map(_.asInstanceOf[SelectOperator]).partition(so => {
        val encPred = getEncodedStr(so.getPredicate)
        propertyDf.hasColumn(encPred)
      })
      val propertyTree = processPropertyTable(inclSo, propertyDf)
      val triplesTree = processTriplesTable(exclSo, triplesDf)
      val condition = Some(analyzedOperators.logicalOperators.ConditionOperator(tripleSubLongField, ConditionType.EQ, tripleSubLongField))
      analyzedOperators.dataOperators.JoinOperator(propertyTree, triplesTree, condition)
    }
    else {
      if (dfs(0).isPropertyTable) {
        processPropertyTable(getChildren(jso).map(_.asInstanceOf[SelectOperator]), dfs(0))
      }
      else {
        processTriplesTable(getChildren(jso).map(_.asInstanceOf[SelectOperator]), dfs(0))
      }
    }
  }

  private def processProjectOperator(po: ProjectOperator, dfO: Option[DataFrame]): analyzedOperators.columnOperators.ProjectOperator = {
    val child = refineBySpatioTemporalInfo(processNode(po.getChild, dfO))
    analyzedOperators.columnOperators.ProjectOperator(child, po.getVariables)
  }

  private def processRenameOperator(ro: RenameOperator, dfO: Option[DataFrame]): analyzedOperators.columnOperators.RenameOperator = {
    val child = processNode(ro.getChild, dfO)
    analyzedOperators.columnOperators.RenameOperator(child, ro.getColumnMapping.asScala.toArray.map(x => (x._1.getColumnName, x._2.getColumnName)))
  }

  private def processNode(node: BaseOperator, dfO: Option[DataFrame]): analyzedOperators.commonOperators.BaseOperator = {
    node match {
      case so: SelectOperator => processSelectOperator(so, dfO)
      case so: SortOperator => processSortOperator(so, dfO)
      case uo: UnionOperator => processUnionOperator(uo, dfO)
      case lo: LimitOperator => processLimitOperator(lo, dfO)
      case jo: JoinOperator => processJoinOperator(jo, dfO)
      case js: JoinSubjectOperator => processJoinSubjectOperator(js, dfO)
      case po: ProjectOperator => processProjectOperator(po, dfO)
      case ro: RenameOperator => processRenameOperator(ro, dfO)
      case _ => throw new Exception("Not supported operator")
    }
  }

  def analyzePlan(root: BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    processNode(root, None)
  }
}
