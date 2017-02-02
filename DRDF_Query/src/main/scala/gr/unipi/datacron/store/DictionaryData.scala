package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.RegexUtils._

class DictionaryData(config: Config, expData: ExpData) {
  val data = expData.hdfs.sc.textFile(config.getString(Consts.qfpDicPath))
  
  def getIdByValue(value: String): String = {
    val searchStr = ("^-?\\d+" + Consts.dicFieldsSeparator +  value).r
    val result = data.filter(searchStr.matches(_)).collect()
    if (result.size == 0)
      return null
    else
      return result(0).substring(0, result(0).indexOf(Consts.dicFieldsSeparator))
  }
  
  def getValueById(id: String): String = {
    val searchStr = ("^" + id + Consts.dicFieldsSeparator + ".*").r
    val result = data.filter(searchStr.matches(_)).collect()
    if (result.size == 0)
      return null
    else
      return result(0).substring(result(0).indexOf(Consts.dicFieldsSeparator) + 1)
  }
  
  def getValuesListByIdsList(ids: Array[(Long, Long)]): scala.collection.Map[Long, String] = {
    val result = data.flatMap(x => {
      val id = x.substring(0, x.indexOf(Consts.dicFieldsSeparator)).toLong
      val fnc = (() => {
        val flt = ids.filter(_._2 == id)
        if (flt.length == 0) {
          null
        }
        else {
          flt
        }
      })
      val kv = fnc()
      if (kv == null) {
        None
      }
      else {
        val uri = x.substring(x.indexOf(Consts.dicFieldsSeparator) + 1, x.length)
        kv.map(x => {
          (x._1, uri)
        })
        //Array((kv._1, uri))
      }
    })
    result.collectAsMap()
  }
}