package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.Consts
import org.apache.spark.sql.DataFrame

class DictionaryData(config: Config) {
  import DataStore.spark.implicits._

  val data: DataFrame = config.getString(qfpParseDictionary) match {
    case Consts.parseString => DataStore.spark.read.text(config.getString(qfpDicPath)).toDF(dicLineStrField)
    case Consts.parseLongString => DataStore.spark.read.text(config.getString(qfpDicPath)).map(s => {
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