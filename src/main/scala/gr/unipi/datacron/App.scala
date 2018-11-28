package gr.unipi.datacron

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import gr.unipi.datacron.queries._

object App {
  def main(args: Array[String]) {
    AppConfig.init(args(0))

    val query = AppConfig.getString(Consts.qfpQueryType) match {
      case `starSptRangeQuery` => Some(StarSptRangeQuery())
      case `twoHopSptRangeQuery` => Some(TwoHopSptRangeQuery())
      case `chainSptRangeQuery` => Some(ChainQuery())
      case `sparqlQuery` => Some(SparqlQuery())
      case _ => None
    }

    if (query.isDefined) {
      query.get.execute()
    }
  }

}
