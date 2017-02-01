package gr.unipi.datacron

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import java.io.File
import gr.unipi.datacron.queries._
import gr.unipi.datacron.store._

object App {
  
  def printUsage() {
    println("Expected arguments: <path_to_queries_directory>.")
  }
  
  def processQueryFile(queryFile: File): Boolean = {
    new StrQuery(ConfigFactory.parseFile(queryFile)).executeQuery
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
