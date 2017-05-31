package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.Consts
import gr.unipi.datacron.store.DataStore.config
import org.apache.spark.sql.DataFrame

class DictionaryData(config: Config) {
  import DataStore.spark.implicits._

  val dicPath: String = if (config.getString(qfpSparkMaster).equals("yarn")) {
    config.getString(qfpNamenode) + config.getString(qfpHdfsPrefix) + config.getString(qfpDicPath)
  }
  else {
    config.getString(qfpDicPath)
  }

  val data: DataFrame = config.getString(qfpParseDictionary) match {
    case Consts.parseString => DataStore.spark.read.text(dicPath).toDF(dicLineStrField)
    case Consts.parseLongString => DataStore.spark.read.text(dicPath).map(s => {
        val line = s.getAs[String](0)
        val pos = line.indexOf(dicFieldsSeparator)
        val key = line.substring(0, pos).toLong
        val value = line.substring(pos + 1)

        (key, value)
      }).toDF(dicKeyLongField, dicValueStrField)
    case _ => throw new Exception("Dictionary parsing setting not found")
  }

  println("Dictionary dataset: " + config.getString(qfpDicPath))
}