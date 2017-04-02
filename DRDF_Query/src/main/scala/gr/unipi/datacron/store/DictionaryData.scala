package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.RegexUtils._

class DictionaryData(config: Config) {
  import DataStore.spark.implicits._
  
  lazy val data = DataStore.spark.read.text(config.getString(qfpDicPath)).map(s => {
    val splitted = s.getAs[String](0).split(dicFieldsSeparator)
    (splitted(0).toLong, splitted(1))
  }).toDF(dicKeyLongField, dicValueStrField)
}