package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.plans.logical.dynamicPlans.columns.SparqlColumn
import gr.unipi.datacron.plans.logical.dynamicPlans.operators._
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.encodeSingleValueParams

abstract class BaseAnalyzer {
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
