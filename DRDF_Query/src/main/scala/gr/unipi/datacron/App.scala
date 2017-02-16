package gr.unipi.datacron

import com.typesafe.config.ConfigFactory
import java.io.File
import gr.unipi.datacron.queries.sptRange._
import gr.unipi.datacron.queries._
import gr.unipi.datacron.common._
import com.typesafe.config.Config

object App {
  
  def printUsage() {
    println("Expected argument: <path_to_queries_directory>.")
  }
  
  def processQueryFile(queryFile: File): Boolean = {
    var query: BaseQuery = null
    val config = ConfigFactory.parseFile(queryFile)
    
    config.getString(Consts.qfpQueryType) match {
      case Consts.spatialFirstSptRangeQuery => query = new SpatialFirst(config)
      case Consts.rdfFirstSptRangeQuery => query = new RdfFirst(config)
      case _ => println("Unexpected query type")
    }
    
    if (query != null) {
      return query.executeQuery
    }
    false
  }
  
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    }
    else {
      List[File]()
    }
  }
  
  def main(args : Array[String]) {
    if (args.length != 1) {
      printUsage()
      System.exit(-1)
    }
    
    val files = getListOfFiles(args(0))
    files.forall(processQueryFile(_))
  }

}
