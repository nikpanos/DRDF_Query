package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns._
import gr.unipi.datacron.plans.logical.dynamicPlans.operands._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._

import scala.collection.mutable
import scala.collection.JavaConverters._


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

  override protected def getPrefixedColumnNameForOperation(oldTreeNode: BaseOperator, c: SparqlColumn, newTreeNode: BaseOperator): String = {
    val prefix = if (isPrefixed(newTreeNode)) {
      prefixMappings(getPrefix(c.getColumnName))
    }
    else { "" }
    val suffix = c.getColumnTypes match {
      case SUBJECT => tripleSubLongField
      case PREDICATE => throw new Exception("Does not support operation on Predicate columns")
      case OBJECT =>
        val fil = c.getColumnName.substring(0, c.getColumnName.indexOf('.')) + ".Predicate"
        val colName = oldTreeNode.getArrayColumns.find(c => c.getColumnName.equals(fil)).get.getQueryString
        getEncodedStr(colName)
    }
    prefix + suffix
  }

  private def isPrefixed(node: BaseOperator): Boolean = node match {
    case _: PrefixOperator => true
    case _: DatasourceOperator => false
    case o: BaseOperator => o.getBopChildren.exists(isPrefixed)
  }

  override protected def prefixNode(oldTreeNode: BaseOperator, col: SparqlColumn, newTreeNode: BaseOperator): BaseOperator = {
    if (isPrefixed(newTreeNode)) {
      newTreeNode
    }
    else {
      val pref = getPrefix(col.getColumnName)
      oldTreeNode.getArrayColumns.foreach(c => { prefixMappings.put(getPrefix(c.getColumnName), pref) })
      PrefixOperator(newTreeNode, pref)
    }
  }

  override protected def processJoinOperator(jo: JoinOperator): BaseOperator = {
    val operand = jo.getJoinOperand.asInstanceOf[OperandPair]
    val leftColumnOperand = operand.getLeftOperand.asInstanceOf[ColumnOperand]
    val rightColumnOperand = operand.getRightOperand.asInstanceOf[ColumnOperand]

    val leftChild = prefixNode(jo.getLeftChild, leftColumnOperand.getColumn, processNode(jo.getLeftChild))
    val rightChild = prefixNode(jo.getRightChild, rightColumnOperand.getColumn, processNode(jo.getRightChild))

    val leftColName = getPrefixedColumnNameForOperation(jo, leftColumnOperand.getColumn, leftChild)
    val rightColName = getPrefixedColumnNameForOperation(jo, rightColumnOperand.getColumn, rightChild)
    val operandPair = OperandPair.newOperandPair(ColumnNameOperand(leftColName), ColumnNameOperand(rightColName), operand.getConditionType)
    JoinOperator.newJoinOperator(leftChild, rightChild, operandPair)
  }

  override protected def processProjectOperator(po: ProjectOperator): BaseOperator = {
    val child = processNode(po.getChild)
    val columns = po.getVariables.map(v => {
      val col = po.getArrayColumns.find(_.getQueryString == v).get
      (getPrefixedColumnNameForOperation(po, col, child), v)
    })
    val newPo = ProjectOperator.newProjectOperator(child, columns.map(_._1))
    def convertToSparqlColumn(columnName: String): SparqlColumn = SparqlColumn.newSparqlColumn(columnName, "", ColumnTypes.OBJECT)
    val newRo = RenameOperator.newRenameOperator(newPo, columns.map(c => {
      (convertToSparqlColumn(c._1), convertToSparqlColumn(c._2))
    }).toMap.asJava)
    if (AppConfig.getBoolean(qfpEnableResultDecode)) DecodeOperator(newRo)
    else newRo
  }

  override protected def processRenameOperator(ro: RenameOperator): BaseOperator = {
    println(ro.getChild.getClass.getName)
    ro.getColumnMapping.asScala.foreach(x => {
      println("(" + x._1.getColumnName + "," + x._2.getColumnName + ")")
    })
    throw new NotImplementedError("Rename operation is not yet implemented")
  }

  override protected def processSortOperator(so: SortOperator): BaseOperator = {
    val child = processNode(so.getChild)
    val d = so.getColumnWithDirection.map(cwd => ColumnWithDirection.newColumnWithDirection(Column.newColumn(cwd.getColumn.getColumnName), cwd.getDirection))
    SortOperator.newSortOperator(child, d)
  }

  private def processOperand(operand: BaseOperand, oldTreeNode: BaseOperator, newTreeNode: BaseOperator): BaseOperand = operand match {
    case vo: ValueOperand => vo
    case co: ColumnOperand => ColumnNameOperand(getPrefixedColumnNameForOperation(oldTreeNode, co.getColumn, newTreeNode))
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
