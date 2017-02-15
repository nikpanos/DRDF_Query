package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
//import gr.unipi.datacron.common.RegexUtils._
import org.apache.spark.sql.functions._

class TriplesData(config: Config) {
  
  import ExpData.spark.implicits._
  
  lazy val data = ExpData.sc.textFile(config.getString(Consts.qfpTriplesPath)).toDF("spo")
  
  def getObjectBySP(sp: String): String = {
    val searchStr = (sp + Consts.tripleFieldsSeparator + "-?\\d+")
    val result = data.filter($"spo" rlike searchStr).collect()
    if (result.size == 0)
      return null
    else {
      val resStr = result(0).getAs[String]("spo")
      return resStr.substring(resStr.indexOf(Consts.tripleFieldsSeparator) + 1)
    }
  }
  
  def getListOByListSP(spList: Array[(Long, Long)]): Array[(Long, Long)] = {
    val strList = spList.map(x => {x._1.toString + Consts.tripleFieldsSeparator + x._2.toString}).toSet
    val bc  = ExpData.sc.broadcast(strList)
    
    val myNameFilter = udf {(s: String) => {
      val sp = s.substring(0, s.lastIndexOf(Consts.tripleFieldsSeparator))
      (bc.value.contains(sp))
    }}
    
    val result = data.filter(myNameFilter($"spo")).rdd.map(x => {
      val splitted = x.getAs[String]("spo").split(Consts.tripleFieldsSeparator)
      (splitted(0).toLong, splitted(2).toLong)
    }).collect()
    return result
  }
}