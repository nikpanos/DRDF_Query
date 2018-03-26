package gr.unipi.datacron

import gr.unipi.datacron.common._
import gr.unipi.datacron.queries._
import gr.unipi.datacron.common.Consts._

object App {
  def main(args : Array[String]) {
    AppConfig.init(args(0))

    println(AppConfig.getInt(Consts.partitionsNumberAfterShuffle))

    val query = AppConfig.getString(Consts.qfpQueryType) match {
      case `starSptRangeQuery` => Some(StarSptRangeQuery())
      case `twoHopSptRangeQuery` => Some(TwoHopSptRangeQuery())
      case `chainSptRangeQuery` => Some(ChainQuery())
      case _ => None
    }

    if (query.isDefined) {
      query.get.execute()
    }
  }

}
