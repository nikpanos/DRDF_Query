package gr.unipi.datacron

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.store.DataStore.spark
object TextToParquet {

  def printUsage(): Unit = {
    println("4 arguments expected:")
    println("\t1: Path to params file")
    println("\t2: Path to output directory")
    println("\t3: The number of partitions of the output file, or 0 for default")
    println("\t4: 1 for sorting enabled or anything else for sorting disabled")

  }

  def main(args : Array[String]): Unit = {

    if (args.length != 4) {
      printUsage()
      return ;
    }

    val newTriplesPath = args(1)
    val partitionsNum = args(2).toInt
    val sorted = args(3).equals("1")

    AppConfig.init(args(0))
    DataStore.init()

    println("Writing output to: " + newTriplesPath)

    DataStore.triplesData.printSchema()

    val triples = if (sorted) {
      if (partitionsNum == 0) {
        throw new Exception("When sorting, you must specify the number of partitions")
      }
      spark.sql("set spark.sql.shuffle.partitions=" + partitionsNum)
      DataStore.triplesData.sort(tripleSubLongField, triplePredLongField, tripleObjLongField)
    }
    else {
      if (partitionsNum != 0) {
        DataStore.triplesData.repartition(partitionsNum)
      }
      else {
        DataStore.triplesData
      }
    }

    triples.write.parquet(newTriplesPath)

    println("Done!")
  }
}
