package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import java.text.SimpleDateFormat

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.common.{AppConfig, SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.{Column, ColumnTypes, ConditionType}
import gr.unipi.datacron.plans.logical.dynamicPlans.operands.{BaseOperand, ColumnOperand, OperandPair, ValueOperand}
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.encodeSingleValueParams
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try


object PlanAnalyzer extends LowLevelAnalyzer {

  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

  private var shouldApplyExactSpatioTemporalFilterLater = false

  private val prefixMappings: mutable.HashMap[String, String] = mutable.HashMap[String, String]()

  private val constraints = Try(SpatioTemporalRange(
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), AppConfig.getOptionalDouble(qfpAltLower), dateFormat.parse(AppConfig.getString(qfpTimeLower)).getTime),
    SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), AppConfig.getOptionalDouble(qfpAltUpper), dateFormat.parse(AppConfig.getString(qfpTimeUpper)).getTime))).toOption

  private def getChildren(bop: BaseOperator): Array[BaseOperator] = bop.getBopChildren

  private def getPrefix(s: String): String = s.substring(0, s.indexOf('.') + 1)
  private def getSuffix(s: String): String = s.substring(s.indexOf('.') + 1)




  /*private def refineBySpatioTemporalInfo(child: analyzedOperators.commonOperators.BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    if (shouldApplyExactSpatioTemporalFilterLater) {
      shouldApplyExactSpatioTemporalFilterLater = false
      analyzedOperators.spatiotemporalOperators.ExactBoxOperator(child, constraints.get, child.isPrefixed)
    }
    else {
      child
    }
  }

  private def getPushedDownSpatioTemporalOperator(df: DataFrame, child: analyzedOperators.commonOperators.BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    if (constraints.isDefined && df.hasSpatialAndTemporalShortcutCols) {
      val newOp = if (AppConfig.getOptionalBoolean(qfpEnableFilterByEncodedInfo).getOrElse(true)) {
        shouldApplyExactSpatioTemporalFilterLater = true
        analyzedOperators.spatiotemporalOperators.ApproximateBoxOperator(child, constraints.get, child.isPrefixed)
      } else { child }
      if (AppConfig.getBoolean(qfpEnableRefinementPushdown)) {
        refineBySpatioTemporalInfo(child)
      } else { newOp }
    }
    else { child }
  }*/

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
            val po = analyzedOperators.columnOperators.ProjectOperator(selOp, Array(tripleSubLongField, tripleObjLongField), false)
            val newColName = getEncodedStr(findSelectOperator(so, PREDICATE).get.getRightOperand.asInstanceOf[ValueOperand].getValue)
            analyzedOperators.columnOperators.RenameOperator(po, Array((tripleObjLongField, newColName)), false)
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

  def processSortOperator(so: SortOperator, dfO: Option[DataFrame]): analyzedOperators.dataOphttps://www.google.com/search?client=ubuntu&channel=fs&q=spark+row_number&ie=utf-8&oe=utf-8erators.SortOperator = {
    val child = refineBySpatioTemporalInfo(processNode(so.getChild, dfO))
    val colWithDirections = so.getColumnWithDirection.map(cwd => {
      val sortEnum: SortEnums.SortEnum = cwd.getDirection match {
        case 1 => SortEnums.Asc
        case -1 => SortEnums.Desc
        case _ => throw new Exception("Unrecognized sorting direction")
      }
      (cwd.getColumn.getColumnName, sortEnum)
    })

    analyzedOperators.dataOperators.SortOperator(child, colWithDirections, child.isPrefixed)
  }

  def processUnionOperator(uo: UnionOperator, dfO: Option[DataFrame]): analyzedOperators.dataOperators.UnionOperator = {
    val leftChild = refineBySpatioTemporalInfo(processNode(uo.getLeftChild, dfO))
    val rightChild = refineBySpatioTemporalInfo(processNode(uo.getRightChild, dfO))
    analyzedOperators.dataOperators.UnionOperator(Array(leftChild, rightChild), true)
  }

  def processLimitOperator(lo: LimitOperator, dfO: Option[DataFrame]): analyzedOperators.dataOperators.LimitOperator = {
    val child = refineBySpatioTemporalInfo(processNode(lo.getChild, dfO))
    analyzedOperators.dataOperators.LimitOperator(child, lo.getLimit, child.isPrefixed)
  }

  private def getColumnNameForOperation(op: BaseOperator, c: Column, child: analyzedOperators.commonOperators.BaseOperator): String = {
    val prefix = if (child.isPrefixed) {
      prefixMappings(getPrefix(c.getColumnName)) + '.'
    }
    else { "" }
    val suffix = c.getColumnTypes match {
      case SUBJECT => tripleSubLongField
      case PREDICATE => throw new Exception("Does not support operation on Predicate columns")
      case _ =>
        val fil = c.getColumnName.substring(0, c.getColumnName.indexOf('.')) + ".Predicate"
        val colName = op.getArrayColumns.find(c => c.getColumnName.equals(fil)).get.getQueryString
        getEncodedStr(colName)
    }
    prefix + suffix
  }

  /*private def getPrefixForColumn(df: DataFrame, op: BaseOperator, col: Column): (String, DataFrame) = {

    if (!df.isPrefixed) {
      op.getArrayColumns.foreach(c => prefixMappings.put(getPrefix(c.getColumnName), pref))
      (pref, PhysicalPlanner.prefixColumns(prefixColumnsParams(df, pref)))
    }
    else {
      (prefixMappings(pref), df)
    }
  }*/

  private def prefixChild(child: analyzedOperators.commonOperators.BaseOperator, col: Column, op: BaseOperator): analyzedOperators.commonOperators.BaseOperator = {
    if (child.isPrefixed) {
      child
    }
    else {
      val pref = getPrefix(col.getColumnName)
      op.getArrayColumns.foreach(c => { prefixMappings.put(getPrefix(c.getColumnName), pref) })
      analyzedOperators.columnOperators.PrefixOperator(child, pref)
    }
  }

  def processJoinOperator(jo: JoinOperator, dfO: Option[DataFrame]): analyzedOperators.dataOperators.JoinOperator = {
    val leftChild = prefixChild(processNode(jo.getLeftChild, dfO), jo.getLeftColumn, jo)
    val rightChild = prefixChild(processNode(jo.getRightChild, dfO), jo.getRightColumn, jo)

    //val cols = jo.getColumnJoinPredicate
    val leftCol = getColumnNameForOperation(jo, jo.getLeftColumn)
    val rightCol = getColumnNameForOperation(jo, jo.getRightColumn)
    val condition = if (cols.isEmpty) {
      None
    }
    else {
      Some(analyzedOperators.logicalOperators.ConditionOperator(cols(0).getColumnName, ConditionType.EQ, cols(1).getColumnName))
    }
    analyzedOperators.dataOperators.JoinOperator(leftChild, rightChild, condition)
  }

  private def processProjectOperator(po: ProjectOperator): analyzedOperators.columnOperators.ProjectOperator = {
    //val child = refineBySpatioTemporalInfo(processNode(po.getChild, dfO))
    analyzedOperators.columnOperators.ProjectOperator(child, po.getVariables, child.isPrefixed)
  }

  override protected def processRenameOperator(ro: RenameOperator): BaseOperator = {
    val child = processNode(ro.getChild)
    analyzedOperators.columnOperators.RenameOperator(child, ro.getColumnMapping.asScala.toArray.map(x => (x._1.getColumnName, x._2.getColumnName)), child.isPrefixed)
  }




}
