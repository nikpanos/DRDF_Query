package gr.unipi.datacron

import gr.unipi.datacron.common.{AppConfig, Consts}
import gr.unipi.datacron.common.Consts.{myParam, qfpParseTriples}
import gr.unipi.datacron.store.DataStore

object DuplicatesRemoval {

  def printUsage(): Unit = {
    println("2 arguments expected:")
    println("\t1: Path to params file")
    println("\t2: Path to output directory")
  }

  def main(args : Array[String]): Unit = {
    if (args.length != 2) {
      printUsage()
      return ;
    }

    val newTriplesPath = args(1)

    AppConfig.init(args(0))

    if (!AppConfig.getString(qfpParseTriples).equals(Consts.parseString)) {
      println("Triple parsing should be \"S\" (String)")
      return ;
    }

    DataStore.init()

    println("Writing output to: " + newTriplesPath)
    println("Partitions number: " + AppConfig.getInt(myParam))

    val res = DataStore.triplesData.rdd.map(x => {
      val spo = x.getAs[String](0)
      (spo, 0)
    }).reduceByKey((a, b) => {
      a
    }).map((key) => {
      key._1
    }).saveAsTextFile(newTriplesPath)
  }
}
