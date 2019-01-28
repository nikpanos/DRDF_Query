package gr.unipi.datacron.plans.logical.dynamicPlans

import java.text.SimpleDateFormat

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.dynamicPlans.parsing.LogicalPlanner
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SortDirection
import gr.unipi.datacron.plans.logical.dynamicPlans.operands._
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.io.Source

case class DynamicLogicalPlan() extends BaseLogicalPlan() {
  private var rootNode: BaseOperator = _

  override def preparePlan(): Unit = {
    val q = AppConfig.getString(sparqlQuerySource)
    val sparqlQuery = if (q.startsWith("file://")) {
      val filename = q.substring(7)
      Source.fromFile(filename).getLines.mkString(" ")
    }
    else {
      q
    }
    val tree = LogicalPlanner.setSparqlQuery(sparqlQuery).build().getRoot
    println(tree)
    println("\n\n\n\n\n")
    val analyzer = new analyzers.PlanAnalyzer()
    rootNode = analyzer.analyzePlan(tree)
  }

  override def doAfterPrepare(): Unit = println(rootNode)

  override private[logical] def doExecutePlan(): DataFrame = {
    processNode(rootNode).get
  }

  private def processNode(node: BaseOperator): Option[DataFrame] = {
    node match {
      case so: DatasourceOperator => processDatasourceOperator(so)
      case co: DecodeAllOperator => processDecodeAllOperator(co)
      case co: DecodeColumnsOperator => processDecodeColumnsOperator(co)
      case to: DistinctOperator => processDistinctOperator(to)
      case jo: JoinOperator => processJoinOperator(jo)
      case lo: LimitOperator => processLimitOperator(lo)
      case po: PrefixOperator => processPrefixOperator(po)
      case po: ProjectOperator => processProjectOperator(po)
      case ro: RenameOperator => processRenameOperator(ro)
      case so: SelectOperator => processSelectOperator(so)
      case so: SortOperator => processSortOperator(so)
      case uo: UnionOperator => processUnionOperator(uo)
      case o => throw new Exception("Not supported operator in executor: " + o.getClass.getName)
    }
  }

  private def processDatasourceOperator(so: DatasourceOperator): Option[DataFrame] = Some(so.df)

  private def processDecodeAllOperator(so: DecodeAllOperator): Option[DataFrame] = {
    val childDf = processNode(so.getChild).get
    Some(PhysicalPlanner.decodeAllColumnsExceptFor(decodeAllColumnsExceptForParams(childDf, so.exceptForColumns)))
  }

  private def processDecodeColumnsOperator(so: DecodeColumnsOperator): Option[DataFrame] = {
    val childDf = processNode(so.getChild).get
    Some(PhysicalPlanner.decodeColumns(decodeColumnsParams(childDf, so.columnNames, preserveColumnNames = true)))
  }

  private def processDistinctOperator(so: DistinctOperator): Option[DataFrame] = {
    val childDf = processNode(so.getChild).get
    Some(PhysicalPlanner.distinctData(distinctDataParams(childDf)))
  }

  private def processJoinOperator(jo: JoinOperator): Option[DataFrame] = {

    val leftDf = processNode(jo.getLeftChild).get
    val rightDf = processNode(jo.getRightChild).get

    val leftSize = jo.getLeftChild.getOutputSize
    val rightSize = jo.getRightChild.getOutputSize

    jo.getJoinOperand match {
      case pair: OperandPair =>
        val leftOperand = pair.getLeftOperand.asInstanceOf[ColumnNameOperand].columnName
        val rightOperand = pair.getRightOperand.asInstanceOf[ColumnNameOperand].columnName

        Some(PhysicalPlanner.joinDataframes(joinDataframesParams(leftDf, rightDf, leftOperand, rightOperand, leftSize, rightSize)))
      case valOp: ValueOperand =>
        Some(PhysicalPlanner.joinAllDataframes(joinAllDataframesParams(leftDf, rightDf, leftSize, rightSize)))
    }
  }

  private def processLimitOperator(lo: LimitOperator): Option[DataFrame] = {
    val childDf = processNode(lo.getChild).get
    Some(PhysicalPlanner.limitResults(limitResultsParams(childDf, lo.getLimit)))
  }

  private def processPrefixOperator(po: PrefixOperator): Option[DataFrame] = {
    val childDf = processNode(po.getChild).get
    Some(PhysicalPlanner.prefixColumns(prefixColumnsParams(childDf, po.prefix)))
  }

  private def processProjectOperator(po: ProjectOperator): Option[DataFrame] = {
    val childDf = processNode(po.getChild).get
    Some(PhysicalPlanner.selectColumns(selectColumnsParams(childDf, po.getVariables)))
  }

  private def processRenameOperator(ro: RenameOperator): Option[DataFrame] = {
    val childDf = processNode(ro.getChild).get
    val mapping = ro.getColumnMapping.asScala.map(x => (x._1.getColumnName, x._2.getColumnName)).toMap
    Some(PhysicalPlanner.renameColumns(renameColumnsParams(childDf, mapping)))
  }

  private def processSelectOperator(so: SelectOperator): Option[DataFrame] = {

    def getStringFromOperand(operand: BaseOperand): BaseLiteralOperand = operand match {
      case vo: ValueOperand => vo
      case cn: ColumnNameOperand => cn
    }

    val childDf = processNode(so.getChild).get
    val res = so.getOperands.foldLeft(childDf)((df, op) => op match {
      case vo: ValueOperand => PhysicalPlanner.filterByValue(filterByValueParams(df, vo.getValue))
      case cn: ColumnNameOperand => PhysicalPlanner.filterByColumn(filterByColumnParams(df, cn.columnName))
      case op: OperandPair =>
        val left = getStringFromOperand(op.getLeftOperand)
        val right = getStringFromOperand(op.getRightOperand)
        val opPair = LiteralOperandPair(left, right, op.getConditionType)
        PhysicalPlanner.filterByLiteralOperandPair(filterByLiteralOperandPairParams(df, opPair))
      case no: NotNullOperand =>
        PhysicalPlanner.filterNullProperties(filterNullPropertiesParams(df, Array(no.columnName)))
      case o => throw new NotImplementedError("Operand not supported in Select operator: " + o.getClass.getSimpleName)
    })

    /*val childDf = processNode(so.getChild).get
    val res = so.getOperands.foldLeft(childDf)((df, op) => {
      PhysicalPlanner.filterByOperand(filterByOperandParams(df, op))
    })*/
    Some(res)
  }

  private def processSortOperator(so: SortOperator): Option[DataFrame] = {
    val childDf = processNode(so.getChild).get
    val sorting = so.getColumnWithDirection.map(x => (x.getColumn.getColumnName, x.getDirection == SortDirection.ASC))
    Some(PhysicalPlanner.sortResults(sortResultsParams(childDf, sorting)))
  }

  private def processUnionOperator(uo: UnionOperator): Option[DataFrame] = {
    val leftDf = processNode(uo.getLeftChild).get
    val rightDf = processNode(uo.getRightChild).get

    Some(PhysicalPlanner.unionDataframes(unionDataframesParams(leftDf, rightDf)))
  }

}
