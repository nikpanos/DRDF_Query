package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import org.apache.spark.sql.{DataFrame, Row}

private[store] class TriplesData() {
  import DataStore.spark.implicits._

  private[store] val triplesPath: String = if (AppConfig.yarnMode) {
    AppConfig.getString(qfpNamenode) + AppConfig.getString(qfpHdfsPrefix) + AppConfig.getString(qfpTriplesPath)
  }
  else {
    AppConfig.getString(qfpTriplesPath)
  }
  
  private[store] val data: DataFrame = Benchmarks.doBenchmark[DataFrame](() => {AppConfig.getString(qfpParseTriples) match {
    case Consts.parseParquet => DataStore.spark.read.parquet(triplesPath)
    case Consts.parseString => DataStore.spark.read.text(triplesPath).toDF(tripleSpoStrField)
    case Consts.parseTripleLong => DataStore.spark.read.text(triplesPath).map(s => {
      val tokenizer = TriplesTokenizer(s.getString(0))

      val sub = tokenizer.getNextToken
      val pred = tokenizer.getNextToken
      val obj = tokenizer.getNextToken

      (sub.get, pred.get, obj.get)
      }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField)
    case Consts.parseTripleLongProperties => DataStore.spark.read.text(triplesPath).map(s => {
      val tokenizer = TriplesTokenizer(s.getString(0))

      val sub = tokenizer.getNextToken
      val pred = tokenizer.getNextToken
      val obj = tokenizer.getNextToken

      val temporal = tokenizer.getNextToken
      if (temporal.isDefined) {
        val spatial = tokenizer.getNextToken
        val properties = tokenizer.getRestSubstring
        val tmp = (sub.get, pred.get, obj.get, temporal.get, spatial.get, properties)
        tmp
      }
      else {
        (sub.get, pred.get, obj.get, null.asInstanceOf[Long], null.asInstanceOf[Long], null)
      }
    }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField, tripleTimeStartField, tripleMBRField, triplePropertiesStrField)
    case _ => throw new Exception("Triples parsing setting not found")
  }}, new BaseOperatorParams() {
    override def operationName: Option[String] = Some("Load triples dataset")
  })

  println("Triples dataset: " + triplesPath)
}