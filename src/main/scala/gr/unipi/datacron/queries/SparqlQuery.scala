package gr.unipi.datacron.queries
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.plans.logical.dynamicPlans.DynamicLogicalPlan

case class SparqlQuery() extends BaseQuery() {
  override protected[queries] def getExecutionPlan(): Option[BaseLogicalPlan] = Some(DynamicLogicalPlan())
}
