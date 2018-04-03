package gr.unipi.datacron

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.dynamicPlans.DynamicLogicalPlan

object MyTest {
  def main(args : Array[String]) {
    AppConfig.init("params/params.hocon")

    DynamicLogicalPlan().myPlanExecution()
  }
}
