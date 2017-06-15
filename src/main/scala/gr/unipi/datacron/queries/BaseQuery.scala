package gr.unipi.datacron.queries

import gr.unipi.datacron.common.Benchmarks
import gr.unipi.datacron.store.DataStore

abstract class BaseQuery() {
  DataStore.init()

  def execute(): Unit = {
    doBeforeExecution()
    doExecute()
    doAfterExecution()

    Benchmarks.printAll()
  }

  private[queries] def doBeforeExecution(): Unit = {
  }

  private[queries] def doExecute()

  private[queries] def doAfterExecution(): Unit = {
  }
}
