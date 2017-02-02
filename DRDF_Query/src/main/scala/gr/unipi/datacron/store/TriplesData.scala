package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.RegexUtils._

class TriplesData(config: Config, expData: ExpData) {
  val data = expData.hdfs.sc.textFile(config.getString(Consts.qfpTriplesPath))
  
  def getObjectBySP(sp: String): String = {
    val searchStr = (sp + Consts.tripleFieldsSeparator + "-?\\d+").r
    val result = data.filter(searchStr.matches(_)).collect()
    if (result.size == 0)
      return null
    else
      return result(0).substring(result(0).indexOf(Consts.tripleFieldsSeparator) + 1)
  }
  
  def getListOByListSOnConstantP(sList: Array[Long], p: String): Array[(Long, Long)] = {
    val tmpStr = ("^-?\\d+" + Consts.tripleFieldsSeparator + p + Consts.tripleFieldsSeparator + "-?\\d+")
    val searchStr = tmpStr.r
    val result = data.filter(x => {
      if (searchStr.matches(x)) {
        val id = x.substring(0, x.indexOf(Consts.tripleFieldsSeparator)).toLong
        val idx = java.util.Arrays.binarySearch(sList, id)
        (idx >= 0)
      }
      else {
        false
      }
    }).map(x => {
      val splitted = x.split(Consts.tripleFieldsSeparator)
      (splitted(0).toLong, splitted(2).toLong)
    }).collect()
    return result
  }
}