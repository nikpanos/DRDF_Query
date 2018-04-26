package gr.unipi.datacron

import gr.unipi.datacron.common.{AppConfig, Consts, Utils}
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore

object DuplicatesRemoval {

  def main(args : Array[String]): Unit = {
    AppConfig.init(args(0))

    if (!AppConfig.getString(qfpParseTriples).equals(Consts.parseString)) {
      println("Triple parsing should be \"S\" (String)")
      return ;
    }

    DataStore.init()

    println("Writing output to: " + AppConfig.getString(qfpQueryOutputFolderPath))
    println("Partitions number: " + AppConfig.getInt(partitionsNumberAfterShuffle))

    val res = DataStore.triplesData.rdd.map(x => {
      val spo = x.getAs[String](0)
      (spo, 0)
    }).reduceByKey((a, b) => {
      a
    }).map((key) => {
      key._1
    }).saveAsTextFile(Utils.resolveHdfsPath(qfpQueryOutputFolderPath))
  }
}
