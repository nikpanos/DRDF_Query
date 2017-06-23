package gr.unipi.datacron

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore
object TextToParquet {

  def main(args : Array[String]): Unit = {
    AppConfig.init(args(0))
    DataStore.init()

    val triplesPath: String = if (AppConfig.yarnMode) {
      AppConfig.getString(qfpNamenode) + AppConfig.getString(qfpHdfsPrefix) + AppConfig.getString(qfpTriplesPath)
    }
    else {
      AppConfig.getString(qfpTriplesPath)
    }

    val newTriplesPath = triplesPath.substring(0, triplesPath.lastIndexOf('.')) + ".sortedSPO.parquet"
    println("Writing output to: " + newTriplesPath)

    DataStore.triplesData.printSchema()

    DataStore.spark.sql("set spark.sql.shuffle.partitions=11")

    DataStore.triplesData.sort(tripleSubLongField, triplePredLongField, tripleObjLongField).write.parquet(newTriplesPath)

    println("Done!")
  }
}