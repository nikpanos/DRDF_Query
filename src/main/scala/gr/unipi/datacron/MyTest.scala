package gr.unipi.datacron

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.plans.logical.dynamicPlans.DynamicLogicalPlan

object MyTest {
  def main(args : Array[String]) {
    //AppConfig.init("params/params.hocon")

    //DynamicLogicalPlan().executePlan
    val test = Array(1, 2, 3, 4, 5)
    val result = test.tail.foldLeft(test.head)((x: Int, y: Int) => {
      println("x="+x)
      println("y="+y)
      x + y
    })
    println(result)
  }
}
