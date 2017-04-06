package gr.unipi.datacron

import com.typesafe.config.ConfigFactory
import java.io.File
import gr.unipi.datacron.common._
import gr.unipi.datacron.queries.starSTRange._

object App {
  
  def printUsage() {
    println("Expected argument: <path_to_query_file>.")
  }
  
  def processQueryFile(queryFile: File): Boolean = {
    val config = ConfigFactory.parseFile(queryFile)
    
    val query = config.getString(Consts.qfpQueryType) match {
      case Consts.spatialFirstSptRangeQuery => Some(StarSpatialFirst(config))
      case Consts.rdfFirstSptRangeQuery => Some(StarRdfFirst(config))
      case Consts.spatialFirstJoinStSptRangeQuery => Some(StarSpatialFirstJoinST(config))
      case _ => None
    }

    if (query.isDefined) {
      val result = query.get.executeQuery.cache
      result.show
      println(result.count)
      true
    }

    false
  }
  
  def main(args : Array[String]) {
    if (args.length != 1) {
      printUsage()
      System.exit(-1)
    }
    
    processQueryFile(new File(args(0)))
  }

}
