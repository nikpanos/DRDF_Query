package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.RegexUtils._

class DictionaryData(config: Config) {
  import ExpData.spark.implicits._
  
  lazy val data = ExpData.sc.textFile(config.getString(Consts.qfpDicPath)).map(s => {
    val splitted = s.split(Consts.dicFieldsSeparator)
    (splitted(0).toLong, splitted(1))
  }).toDF("key", "value")
  
  def getKeyByValue(value: String): Long = {
    /*val searchStr = ("^-?\\d+" + Consts.dicFieldsSeparator +  value).r
    val result = data.filter(searchStr.matches(_)).collect()
    if (result.size == 0)
      return null
    else
      return result(0).substring(0, result(0).indexOf(Consts.dicFieldsSeparator))*/
    data.filter($"value" === value).first().getAs[Long](0)
  }
  
  def getValueByKey(key: Long): String = {
    /*val searchStr = ("^" + id + Consts.dicFieldsSeparator + ".*").r
    val result = data.filter(searchStr.matches(_)).collect()
    if (result.size == 0)
      return null
    else
      return result(0).substring(result(0).indexOf(Consts.dicFieldsSeparator) + 1)*/
    data.filter($"key" === key).first().getAs[String](1)
  }
  
  def getValuesListByKeysList(keys: Array[(Long, Long)]): scala.collection.Map[Long, String] = {
    val result = data.flatMap(x => {
      val key = x.getAs[Long](0)
      val fnc = (() => {
        val flt = keys.filter(_._2 == key)
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
        val uri = x.getAs[String](1)
        kv.map(x => {
          (x._1, uri)
        })
        //Array((kv._1, uri))
      }
    })
    result.rdd.collectAsMap()
  }
}