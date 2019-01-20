package gr.unipi.datacron.plans.logical.dynamicPlans.analyzers

import gr.unipi.datacron.plans.logical.dynamicPlans.operators._

abstract class BaseAnalyzer {
  def analyzePlan(root: BaseOperator): BaseOperator = {
    processNode(root)
  }

  protected def processNode(node: BaseOperator): BaseOperator = {
    node match {
      case to: DistinctOperator => processDistinctOperator(to)
      case jo: JoinOperator => processJoinOperator(jo)
      case js: JoinSubjectOperator => processJoinSubjectOperator(js)
      case lo: LimitOperator => processLimitOperator(lo)
      case po: ProjectOperator => processProjectOperator(po)
      case ro: RenameOperator => processRenameOperator(ro)
      case so: SelectOperator => if (so.getChild.isInstanceOf[TripleOperator]) processLowLevelSelectOperator(so) else processSelectOperator(so)
      case so: SortOperator => processSortOperator(so)
      case uo: UnionOperator => processUnionOperator(uo)
      case _ => throw new Exception("Not supported operator")
    }
  }

  protected def processDistinctOperator(to: DistinctOperator): BaseOperator
  protected def processJoinOperator(jo: JoinOperator): BaseOperator
  protected def processJoinSubjectOperator(js: JoinSubjectOperator): BaseOperator
  protected def processLimitOperator(lo: LimitOperator): BaseOperator
  protected def processLowLevelSelectOperator(so: SelectOperator): BaseOperator
  protected def processProjectOperator(po: ProjectOperator): BaseOperator
  protected def processRenameOperator(ro: RenameOperator): BaseOperator
  protected def processSelectOperator(so: SelectOperator): BaseOperator
  protected def processSortOperator(so: SortOperator): BaseOperator
  protected def processUnionOperator(uo: UnionOperator): BaseOperator
}
