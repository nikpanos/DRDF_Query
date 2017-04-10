package gr.unipi.datacron.queries

import com.typesafe.config.Config

case class TwoHopSptRangeQuery(config: Config) extends BaseQuery(config) {
  override def execute(): Unit = {

  }
}
