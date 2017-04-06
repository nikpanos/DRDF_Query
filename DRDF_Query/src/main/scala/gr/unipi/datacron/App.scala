package gr.unipi.datacron

import com.typesafe.config.ConfigFactory
import java.io.File
import gr.unipi.datacron.common._
import gr.unipi.datacron.queries._

object App {
  
  def printUsage() {
    println("Expected argument: <path_to_query_file>.")
  }
  
  def processQueryFile(queryFile: File): Unit = {
    val config = ConfigFactory.parseFile(queryFile)
    
    val query = config.getString(Consts.qfpQueryType) match {
      case Consts.starSptRangeQuery => Some(StarSptRangeQuery(config))
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
    
    processQueryFile(new File(args(0)))
  }

}
