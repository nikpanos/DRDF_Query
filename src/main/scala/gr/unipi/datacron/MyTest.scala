package gr.unipi.datacron

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.functions._

import collection.JavaConverters._

object MyTest {
  def main(args : Array[String]) {
    AppConfig.init(args(0))
    DataStore.init()

    DataStore.vesselData.select(col("-7")).distinct().collect().map(_.getAs[Long](0)).map(value => {
      DataStore.dictionaryRedis.getDecodedValue(value).get
    }).foreach(println)
  }
}
