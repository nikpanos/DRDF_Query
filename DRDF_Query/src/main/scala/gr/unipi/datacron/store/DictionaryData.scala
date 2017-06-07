package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import org.apache.spark.sql.DataFrame

class DictionaryData() {
  import DataStore.spark.implicits._

  val dicPath: String = if (AppConfig.yarnMode) {
    AppConfig.getString(qfpHdfsPrefix) + AppConfig.getString(qfpDicPath)
  }
  else {
    AppConfig.getString(qfpDicPath)
  }

  val data: DataFrame = (AppConfig.getString(qfpParseDictionary) match {
    case Consts.parseString => DataStore.spark.read.text(dicPath).toDF(dicLineStrField)
    case Consts.parseLongString => DataStore.spark.read.text(dicPath).map(s => {
        val line = s.getAs[String](0)
        val pos = line.indexOf(dicFieldsSeparator)
        val key = line.substring(0, pos).toLong
        val value = line.substring(pos + 1)

        (key, value)
      }).toDF(dicKeyLongField, dicValueStrField)
    case _ => throw new Exception("Dictionary parsing setting not found")
  }).cache

  println("Dictionary dataset: " + dicPath)
}