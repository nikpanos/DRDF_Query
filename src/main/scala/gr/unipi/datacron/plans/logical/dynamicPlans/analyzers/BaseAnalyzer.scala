package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.encodeSingleValueParams

import scala.collection.mutable

abstract class BaseAnalyzer {

  private var decodedColumns = mutable.Set[String]()

  protected var spatioTemporalBoxFilter: Option[SpatioTemporalRange] = None

  def analyzePlan(root: BaseOperator): BaseOperator = {
    processNode(root)
  }

  protected def getEncodedStr(decodedColumnName: String): String = {
    val result = PhysicalPlanner.encodeSingleValue(encodeSingleValueParams(decodedColumnName))
    if (result.isEmpty){
      throw new Exception("Could not find encoded value for column name: " + decodedColumnName)
    }
    else {
      result.get.toString
    }
  }

  private def isLeafNode(node: SelectOperator): Boolean = {
    node.getChild match {
      case ro: RenameOperator => ro.getChild.isInstanceOf[TripleOperator]
      case _ => false
    }
  }

  protected def decodeColumns(child: BaseOperator, columnNames: Array[String]): BaseOperator = {
    val notDecodedColumns = columnNames.filterNot(decodedColumns.contains)
    if (notDecodedColumns.length > 0) {
      notDecodedColumns.foreach(decodedColumns.add)
      DecodeColumnsOperator(child, notDecodedColumns)
    }
    else {
      child
    }
  }

  protected def decodeAllColumns(child: BaseOperator): BaseOperator = DecodeAllOperator(child, decodedColumns.toArray)

  protected def renameDecodedColumns(oldAndNewColumnNames: Map[String, String]): Unit = {
    decodedColumns = decodedColumns.map(c => oldAndNewColumnNames.getOrElse(c, c))
  }

  protected def processNode(node: BaseOperator): BaseOperator = {
    node match {
      case to: DistinctOperator => processDistinctOperator(to)
      case jo: JoinOperator => processJoinOperator(jo)
      case js: JoinSubjectOperator => processJoinSubjectOperator(js)
      case lo: LimitOperator => processLimitOperator(lo)
      case po: ProjectOperator => processProjectOperator(po)
      case ro: RenameOperator => processRenameOperator(ro)
      case so: SelectOperator => if (isLeafNode(so)) processLeafSelectOperator(so) else processSelectOperator(so)
      case so: SortOperator => processSortOperator(so)
      case uo: UnionOperator => processUnionOperator(uo)
      case _ => throw new Exception("Not supported operator in analyzer")
    }
  }

  protected def prefixNode(oldTreeNode: BaseOperator, col: SparqlColumn, newTreeNode: BaseOperator): BaseOperator
  protected def getPrefixedColumnNameForOperation(oldTreeNode: BaseOperator, c: SparqlColumn, newTreeNode: BaseOperator): String

  protected def processDistinctOperator(to: DistinctOperator): BaseOperator
  protected def processJoinOperator(jo: JoinOperator): BaseOperator
  protected def processJoinSubjectOperator(js: JoinSubjectOperator): BaseOperator
  protected def processLimitOperator(lo: LimitOperator): BaseOperator
  protected def processLeafSelectOperator(so: SelectOperator): BaseOperator
  protected def processProjectOperator(po: ProjectOperator): BaseOperator
  protected def processRenameOperator(ro: RenameOperator): BaseOperator
  protected def processSelectOperator(so: SelectOperator): BaseOperator
  protected def processSortOperator(so: SortOperator): BaseOperator
  protected def processUnionOperator(uo: UnionOperator): BaseOperator
}
