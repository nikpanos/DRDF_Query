package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import java.text.SimpleDateFormat

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.{SparqlColumn, ConditionType}
import gr.unipi.datacron.plans.logical.dynamicPlans.operands._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.common.Utils._
import scala.collection.mutable


class PlanAnalyzer extends LowLevelAnalyzer {
  private val prefixMappings: mutable.HashMap[String, String] = mutable.HashMap[String, String]()
  private def getChildren(bop: BaseOperator): Array[BaseOperator] = bop.getBopChildren

  private def getPrefix(s: String): String = s.substring(0, s.indexOf('.') + 1)
  private def getSuffix(s: String): String = s.substring(s.indexOf('.') + 1)

  private def getOperandStr(op: BaseOperand): String = {
    op match {
      case co: ColumnOperand => co.getColumn.getColumnName
      case vo: ValueOperand => vo.getValue
      case _ => throw new Exception("Not supported Operand!")
    }
  }

  private def getColumnNameForOperation(oldTreeNode: BaseOperator, c: SparqlColumn, newTreeNode: BaseOperator): String = {
    val prefix = if (isPrefixed(newTreeNode)) {
      prefixMappings(getPrefix(c.getColumnName)) + '.'
    }
    else { "" }
    val suffix = c.getColumnTypes match {
      case SUBJECT => tripleSubLongField
      case PREDICATE => throw new Exception("Does not support operation on Predicate columns")
      case OBJECT =>
        val fil = c.getColumnName.substring(0, c.getColumnName.indexOf('.')) + ".Predicate"
        val colName = oldTreeNode.getArrayColumns.find(c => c.getColumnName.equals(fil)).get.getColumnName
        getEncodedStr(colName)
    }
    prefix + suffix
  }

  /*private def getPrefixForColumn(df: DataFrame, op: BaseOperator, col: SparqlColumn): (String, DataFrame) = {

    if (!df.isPrefixed) {
      op.getArrayColumns.foreach(c => prefixMappings.put(getPrefix(c.getColumnName), pref))
      (pref, PhysicalPlanner.prefixColumns(prefixColumnsParams(df, pref)))
    }
    else {
      (prefixMappings(pref), df)
    }
  }*/

  private def isPrefixed(node: BaseOperator): Boolean = node match {
    case _: PrefixOperator => true
    case _: DatasourceOperator => false
    case o: BaseOperator => o.getBopChildren.exists(isPrefixed)
  }

  private def prefixNode(oldTreeNode: BaseOperator, colOp: ColumnOperand, newTreeNode: BaseOperator): BaseOperator = {
    if (isPrefixed(newTreeNode)) {
      newTreeNode
    }
    else {
      val pref = getPrefix(colOp.getColumn.getColumnName)
      oldTreeNode.getArrayColumns.foreach(c => { prefixMappings.put(getPrefix(c.getColumnName), pref) })
      PrefixOperator(newTreeNode, pref)
    }
  }

  override protected def processJoinOperator(jo: JoinOperator): BaseOperator = {
    val operand = jo.getJoinOperand.asInstanceOf[OperandPair]
    val leftColumnOperand = operand.getLeftOperand.asInstanceOf[ColumnOperand]
    val rightColumnOperand = operand.getRightOperand.asInstanceOf[ColumnOperand]

    val leftChild = prefixNode(jo, leftColumnOperand, processNode(jo.getLeftChild))
    val rightChild = prefixNode(jo, rightColumnOperand, processNode(jo.getRightChild))

    val leftColName = getColumnNameForOperation(jo, leftColumnOperand.getColumn, leftChild)
    val rightColName = getColumnNameForOperation(jo, rightColumnOperand.getColumn, rightChild)
    val operandPair = OperandPair.newOperandPair(ColumnNameOperand(leftColName), ColumnNameOperand(rightColName), operand.getConditionType)
    JoinOperator.newJoinOperator(leftChild, rightChild, operandPair)
  }

  override protected def processProjectOperator(po: ProjectOperator): BaseOperator = {
    //val child = refineBySpatioTemporalInfo(processNode(po.getChild, dfO))
    //analyzedOperators.columnOperators.ProjectOperator(child, po.getVariables, child.isPrefixed)
  }

  override protected def processRenameOperator(ro: RenameOperator): BaseOperator = {
    //val child = processNode(ro.getChild)
    //analyzedOperators.columnOperators.RenameOperator(child, ro.getColumnMapping.asScala.toArray.map(x => (x._1.getColumnName, x._2.getColumnName)), child.isPrefixed)
  }

  override protected def processSortOperator(so: SortOperator): BaseOperator = {
    so.getColumnWithDirection.map(cwd => cwd.getColumn.getColumnName)
  }

  private def processOperand(operand: BaseOperand, oldTreeNode: BaseOperator, newTreeNode: BaseOperator): BaseOperand = operand match {
    case vo: ValueOperand => vo
    case co: ColumnOperand => ColumnNameOperand(sanitize(getColumnNameForOperation(oldTreeNode, co.getColumn, newTreeNode)))
    case op: OperandPair => OperandPair.newOperandPair(processOperand(op.getLeftOperand, oldTreeNode, newTreeNode), processOperand(op.getRightOperand, oldTreeNode, newTreeNode), op.getConditionType)
    case of: OperandFunction => throw new Exception("Functions are not yet supported")
  }

  override protected def processSelectOperator(so: SelectOperator): BaseOperator = {
    val child = processNode(so.getChild)

    val operands = so.getOperands.map(op => processOperand(op, so, child))
    SelectOperator.newSelectOperator(child, so.getArrayColumns, operands, so.getOutputSize)
  }

  override protected def processLimitOperator(lo: LimitOperator): BaseOperator = {
    val child = processNode(lo.getChild)
    LimitOperator.newLimitOperator(child, lo.getLimit)
  }

  override protected def processUnionOperator(uo: UnionOperator): BaseOperator = {
    val leftChild = processNode(uo.getLeftChild)
    val rightChild = processNode(uo.getRightChild)
    UnionOperator.newUnionOperator(leftChild, rightChild)
  }

  override protected def processDistinctOperator(to: DistinctOperator): BaseOperator = {
    val child = processNode(to.getChild)
    DistinctOperator.newDistinctOperator(child)
  }


  //protected def processDistinctOperator(to: DistinctOperator): BaseOperator
  //protected def processJoinOperator(jo: JoinOperator): BaseOperator
  //protected def processJoinSubjectOperator(js: JoinSubjectOperator): BaseOperator
  //protected def processLimitOperator(lo: LimitOperator): BaseOperator
  //protected def processLowLevelSelectOperator(so: SelectOperator): BaseOperator
  //protected def processProjectOperator(po: ProjectOperator): BaseOperator
  //protected def processRenameOperator(ro: RenameOperator): BaseOperator
  //protected def processSelectOperator(so: SelectOperator): BaseOperator
  //protected def processSortOperator(so: SortOperator): BaseOperator
  //protected def processUnionOperator(uo: UnionOperator): BaseOperator




}
