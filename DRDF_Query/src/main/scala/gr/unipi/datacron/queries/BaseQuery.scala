package gr.unipi.datacron.queries

import gr.unipi.datacron.store.DataStore

abstract class BaseQuery() {
  DataStore.init()

  def execute()
}
