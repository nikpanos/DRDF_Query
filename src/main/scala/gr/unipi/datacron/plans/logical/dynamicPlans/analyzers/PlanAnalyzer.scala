package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.ColumnTypes._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns._
import gr.unipi.datacron.plans.logical.dynamicPlans.functions.RegisteredFunctions
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
    jo.getJoinOperand match {
      case operand: PairOperand =>
        val leftColumnOperand = operand.getLeftOperand.asInstanceOf[ColumnOperand]
        val rightColumnOperand = operand.getRightOperand.asInstanceOf[ColumnOperand]

        val leftChild = prefixNode(jo.getLeftChild, leftColumnOperand.getColumn, processNode(jo.getLeftChild))
        val rightChild = prefixNode(jo.getRightChild, rightColumnOperand.getColumn, processNode(jo.getRightChild))

        val leftColName = getPrefixedColumnNameForOperation(jo, leftColumnOperand.getColumn, leftChild)
        val rightColName = getPrefixedColumnNameForOperation(jo, rightColumnOperand.getColumn, rightChild)
        val operandPair = PairOperand.newOperandPair(ColumnNameOperand(leftColName), ColumnNameOperand(rightColName), operand.getConditionType)
        JoinOperator.newJoinOperator(leftChild, rightChild, operandPair)
      case operand: ValueOperand =>
        val leftChild = prefixNode(jo.getLeftChild, jo.getLeftChild.getArrayColumns()(0), processNode(jo.getLeftChild))
        val rightChild = prefixNode(jo.getRightChild, jo.getRightChild.getArrayColumns()(0), processNode(jo.getRightChild))
        JoinOperator.newJoinOperator(leftChild, rightChild, operand)
    }
    //val operand = jo.getJoinOperand.asInstanceOf[PairOperand]

  }

  override protected def processProjectOperator(po: ProjectOperator): BaseOperator = {
    val child = processNode(po.getChild)
    val columns = po.getVariables.map(v => {
      val col = po.getArrayColumns.find(_.getQueryString == v).get
      (getPrefixedColumnNameForOperation(po, col, child), v)
    })
    val newPo = ProjectOperator.newProjectOperator(child, columns.map(_._1))
    def convertToSparqlColumn(columnName: String): SparqlColumn = SparqlColumn.newSparqlColumn(columnName, "", ColumnTypes.OBJECT)
    val oldAndNewColumns = columns.map(c => {
      (convertToSparqlColumn(c._1), convertToSparqlColumn(c._2))
    }).toMap
    val newRo = RenameOperator.newRenameOperator(newPo, oldAndNewColumns.asJava)
    renameDecodedColumns(oldAndNewColumns.map(x => (x._1.getColumnName, x._2.getColumnName)))
    if (AppConfig.getBoolean(qfpEnableResultDecode)) decodeAllColumns(newRo)
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
    val d = so.getColumnWithDirection.map(cwd => {
      ColumnWithDirection.newColumnWithDirection(Column.newColumn(getPrefixedColumnNameForOperation(so, cwd.getColumn.asInstanceOf[SparqlColumn], child)), cwd.getDirection)
    })
    val columnNames = d.map(_.getColumn.getColumnName)
    SortOperator.newSortOperator(decodeColumns(child, columnNames), d)
  }

  private def processOperand(operand: BaseOperand, oldTreeNode: BaseOperator, newTreeNode: BaseOperator): BaseOperand = operand match {
    case vo: ValueOperand => vo
    case co: ColumnOperand => ColumnNameOperand(getPrefixedColumnNameForOperation(oldTreeNode, co.getColumn, newTreeNode))
    case op: PairOperand => PairOperand.newOperandPair(processOperand(op.getLeftOperand, oldTreeNode, newTreeNode), processOperand(op.getRightOperand, oldTreeNode, newTreeNode), op.getConditionType)
    case of: FunctionOperand =>
      val funcOperands = of.getArguments.map(op => processOperand(op, oldTreeNode, newTreeNode))
      FunctionOperand.newOperandFunction(of.getFunctionName, funcOperands :_*)
  }

  private def decodeColumnsByOperands(operand: BaseOperand, oldTreeNode: BaseOperator, newTreeNode: BaseOperator): BaseOperator = operand match {
    case vo: ValueOperand => newTreeNode
    case co: ColumnOperand => decodeColumns(newTreeNode, Array(getPrefixedColumnNameForOperation(oldTreeNode, co.getColumn, newTreeNode)))
    case op: PairOperand => decodeColumnsByOperands(op.getLeftOperand, oldTreeNode, decodeColumnsByOperands(op.getRightOperand, oldTreeNode, newTreeNode))
    case of: FunctionOperand =>
      val rf = RegisteredFunctions.findFunctionByName(of.getFunctionName)
      if (rf.isEmpty) {
        throw new Exception("Function " + of.getFunctionName + " is not registered!")
      }
      if (rf.get.args.length!= of.getArguments.length) {
        throw new Exception("Function " + of.getFunctionName + " is registered with " + rf.get.args.length + " arguments!")
      }
      val argsForDecode = of.getArguments.zipWithIndex.filter(a => {
        rf.get.args(a._2).needsDecoding
      }).map(_._1)
      if (argsForDecode.length > 0) {
        val firstOp = decodeColumnsByOperands(argsForDecode.head, oldTreeNode, newTreeNode)
        argsForDecode.tail.foldLeft(firstOp)((operator, operand) => {
          decodeColumnsByOperands(operand, oldTreeNode, operator)
        })
      }
      else {
        newTreeNode
      }
  }

  override protected def processSelectOperator(so: SelectOperator): BaseOperator = {
    val childHead = decodeColumnsByOperands(so.getOperands.head, so, processNode(so.getChild))

    val newChild = so.getOperands.tail.foldLeft(childHead)((operator, operand) => decodeColumnsByOperands(operand, so, operator))

    val operands = so.getOperands.map(op => processOperand(op, so, newChild))
    SelectOperator.newSelectOperator(newChild, so.getArrayColumns, operands, so.getOutputSize)
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
