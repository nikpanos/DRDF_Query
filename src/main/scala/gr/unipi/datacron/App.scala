package gr.unipi.datacron

import gr.unipi.datacron.common._
import gr.unipi.datacron.queries._

object App {
  
  def printUsage() {
    println("Expected argument: <path_to_query_file>.")
  }
  
  def processQueryFile(queryFile: String): Unit = {
    AppConfig.init(queryFile)

    val query = AppConfig.getString(Consts.qfpQueryType) match {
      case Consts.starSptRangeQuery => Some(StarSptRangeQuery())
      case Consts.twoHopSptRangeQuery => Some(TwoHopSptRangeQuery())
      case Consts.chainSptRangeQuery => Some(ChainQuery())
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
