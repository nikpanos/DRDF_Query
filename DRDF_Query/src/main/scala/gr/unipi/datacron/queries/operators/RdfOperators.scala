package gr.unipi.datacron.queries.operators

import org.apache.spark.rdd.RDD
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.RegexUtils._
import gr.unipi.datacron.store._

object RdfOperators {
  
  def simpleFilter(rdd: RDD[String], spoFilter: SPO): RDD[String] = {
    val searchStr = spoFilter.getRegExpString
    return rdd.filter(searchStr.matches(_))
  }
}