package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
//import gr.unipi.datacron.common.RegexUtils._
import org.apache.spark.sql.functions._

class TriplesData(config: Config) {
  
  import DataStore.spark.implicits._
  
  lazy val data = DataStore.spark.read.text(config.getString(qfpTriplesPath)).toDF(tripleSpoStrField)
  
  def getListOByListSP(spList: Array[(Long, Long)]): Array[(Long, Long)] = {
    val strList = spList.map(x => {x._1.toString + tripleFieldsSeparator + x._2.toString}).toSet
    val bc  = DataStore.sc.broadcast(strList)
    
    val myNameFilter = udf {(s: String) => {
      val sp = s.substring(0, s.lastIndexOf(tripleFieldsSeparator))
      (bc.value.contains(sp))
    }}
    
    val result = data.filter(myNameFilter(data(tripleSpoStrField))).map(x => {
      val splitted = x.getAs[String](tripleSpoStrField).split(tripleFieldsSeparator)
      (splitted(0).toLong, splitted(2).toLong)
    }).collect()
    return result
  }
}