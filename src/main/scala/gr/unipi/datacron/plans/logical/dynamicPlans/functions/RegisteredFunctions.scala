package gr.unipi.datacron.plans.logical.dynamicPlans.functions

import scala.collection.mutable

object RegisteredFunctions {

  private val functions: mutable.Map[String, RegisteredFunction] = mutable.Map[String, RegisteredFunction]()

  def registerFunction(function: RegisteredFunction): Unit = {
    if (functions.contains(function.functionName)) {
      throw new Exception("Function: " + function.functionName + " is already registered.")
    }
    functions.put(function.functionName, function)
  }

  def findFunctionByName(functionName: String): Option[RegisteredFunction] = {
    functions.get(functionName)
  }

  def registerNativeFunctions(): Unit = {

  }

}
