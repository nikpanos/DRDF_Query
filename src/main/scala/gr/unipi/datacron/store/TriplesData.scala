package gr.unipi.datacron.store

import java.lang.Exception

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import org.apache.spark.sql.DataFrame

import scala.util.control.Exception

private[store] class TriplesData() extends BaseHDFSStore {
  import DataStore.spark.implicits._

  protected def configPropertyForDataPath: String = qfpTriplesPath

  protected def datasetName: String = "Leftover Triples"

  protected def readDataset(): DataFrame = AppConfig.getString(qfpParseTriples) match {
    case Consts.parseParquet => DataStore.spark.read.parquet(dataPath).union(DataStore.spark.read.parquet(Utils.resolveHdfsPath(qfpNodeLeftoversPath)))
    case Consts.parseString => DataStore.spark.read.text(dataPath).toDF(tripleSpoStrField)
    case Consts.parseLong => DataStore.spark.read.text(dataPath).map(s => {
      val tokenizer = TriplesTokenizer(s.getString(0))

      val sub = tokenizer.getNextToken
      val pred = tokenizer.getNextToken
      val obj = tokenizer.getNextToken

      (sub.get, pred.get, obj.get)
      }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField)
    case _ => throw new Exception("Triples parsing setting not found")
  }
}

