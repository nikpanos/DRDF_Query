package gr.unipi.datacron.queries

import com.typesafe.config.Config

abstract class BaseQuery(config: Config) {
  def execute()
}
