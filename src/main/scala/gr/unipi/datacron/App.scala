package gr.unipi.datacron

import gr.unipi.datacron.common._
import gr.unipi.datacron.queries._
import gr.unipi.datacron.common.Consts._

object App {
  
  def printUsage() {
    println("Expected argument: <path_to_query_file>.")
  }
  
  def processQueryFile(queryFile: String): Unit = {
    AppConfig.init()

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
  
  def main(args : Array[String]) {
    if (args.length != 1) {
      printUsage()
      System.exit(-1)
    }
    
    processQueryFile(args(0))
  }

}
