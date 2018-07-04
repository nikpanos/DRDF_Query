package gr.unipi.datacron

import gr.unipi.datacron.common.{AppConfig, Consts}
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.store.DataStore.spark
import org.apache.spark.sql.DataFrame
object TextToParquet {
  private def processDataframe(df: DataFrame, dfName: String, outputPath: String, sortingColumns: Option[Array[String]]): Unit = {
    println("Writing " + dfName + " to: " + outputPath)
    df.printSchema()

    val partitionsNum = AppConfig.getOptionalInt(partitionsNumberAfterShuffle).getOrElse(0)

    val data = if ((sortingColumns.isDefined) && (sortingColumns.get.length > 0)) {
      val cols = sortingColumns.get
      if (partitionsNum == 0) {
        throw new Exception("When sorting, you must specify the number of partitions")
      }
      //spark.sql("set spark.sql.shuffle.partitions=" + partitionsNum)
      df.sort(cols(0), cols.drop(1):_*)
    }
    else if (partitionsNum != 0) {
      df.repartition(AppConfig.getInt(partitionsNumberAfterShuffle))
    }
    else {
      df
    }

    data.write.parquet(outputPath)

    println("Completed " + dfName + "\n")
  }

  def main(args : Array[String]): Unit = {
    AppConfig.init(args(0))
    DataStore.init()

    val sorted = AppConfig.getOptionalBoolean(qfpQueryOutputShouldBeSorted).getOrElse(false)

    var sortCols = if (sorted) {
      Some(Array(tripleSubLongField, triplePredLongField, tripleObjLongField))
    }
    else {
      None
    }

    var outputPath = DataStore.triples.dataPath.replace("/text/", "/parquet/")  //TODO: dirty, maybe clean it
    processDataframe(DataStore.triplesData, "triples", outputPath, sortCols)

    if (AppConfig.getOptionalBoolean(Consts.qfpDataPropertyEnabled).getOrElse(true)) {

      sortCols = if (sorted) {
        Some(Array(tripleSubLongField))
      }
      else {
        None
      }

      AppConfig.getString(qfpDatasetList).split(",").foreach( {
        case `datasetAisMedNode` =>
          outputPath = DataStore.node.dataPath.replace("/text/", "/parquet/") //TODO: dirty, maybe clean it
          processDataframe(DataStore.nodeData, "node", outputPath, sortCols)
        case `datasetVessel` =>
          outputPath = DataStore.vessels.dataPath.replace("/text/", "/parquet/") //TODO: dirty, maybe clean it
          processDataframe(DataStore.vesselData, "vessels", outputPath, sortCols)
      })
    }

    println("Success!")
  }
}
