package gr.unipi.datacron

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.store.DataStore.spark
import org.apache.spark.sql.DataFrame
object TextToParquet {

  def printUsage(): Unit = {
    println("3 arguments expected:")
    println("\t1: Path to params file")
    println("\t2: The number of partitions of the output file, or 0 for default")
    println("\t3: 1 for sorting enabled or anything else for sorting disabled")
  }

  def processDataframe(df: DataFrame, dfName: String, outputPath: String, partitionsNum: Int, sortingColumns: Option[Array[String]]) = {
    println("Writing " + dfName + " to: " + outputPath)
    df.printSchema()

    val data = if ((sortingColumns.isDefined) && (sortingColumns.get.length > 0)) {
      val cols = sortingColumns.get
      if (partitionsNum == 0) {
        throw new Exception("When sorting, you must specify the number of partitions")
      }
      spark.sql("set spark.sql.shuffle.partitions=" + partitionsNum)
      df.sort(cols(0), cols.drop(1):_*)
    }
    else if (partitionsNum != 0) {
      df.repartition(partitionsNum)
    }
    else {
      df
    }

    data.write.parquet(outputPath)

    println("Completed " + dfName + "\n")
  }

  def main(args : Array[String]): Unit = {

    if (args.length != 3) {
      printUsage()
      return ;
    }

    val partitionsNum = args(1).toInt
    val sorted = args(2).equals("1")

    AppConfig.init(args(0))
    DataStore.init()

    var sortCols = if (sorted) {
      Some(Array(tripleSubLongField, triplePredLongField, tripleObjLongField))
    }
    else {
      None
    }

    var outputPath = DataStore.triples.dataPath.replace("/text/", "/parquet/")  //TODO: dirty, maybe clean it
    processDataframe(DataStore.triplesData, "triples", outputPath, partitionsNum, sortCols)

    sortCols = if (sorted) {
      Some(Array(tripleSubLongField))
    }
    else {
      None
    }

    outputPath = DataStore.node.dataPath.replace("/text/", "/parquet/")  //TODO: dirty, maybe clean it
    processDataframe(DataStore.nodeData, "node", outputPath, partitionsNum, sortCols)

    println("Success!")
  }
}
