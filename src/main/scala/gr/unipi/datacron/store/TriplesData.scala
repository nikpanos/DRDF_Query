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
      val line = s.getString(0)

      val sub = TriplesParsingUtils.getNextToken(line, Some(0))
      val pred = TriplesParsingUtils.getNextToken(line, sub._1)
      val obj = TriplesParsingUtils.getNextToken(line, pred._1)

      (sub._2, pred._2, obj._2)
      }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField)
    case Consts.parseTripleLongProperties => DataStore.spark.read.text(triplesPath).map(s => {
      val line = s.getString(0)

      val sub = TriplesParsingUtils.getNextToken(line, Some(0))
      val pred = TriplesParsingUtils.getNextToken(line, sub._1)
      val obj = TriplesParsingUtils.getNextToken(line, pred._1)

      if (obj._1.isDefined) {
        val temporal = TriplesParsingUtils.getNextToken(line, obj._1)
        val spatial = TriplesParsingUtils.getNextToken(line, temporal._1)
        val properties = line.substring(spatial._1.get)
        val tmp = (sub._2, pred._2, obj._2, temporal._2, spatial._2, properties)
        tmp
      }
      else {
        (sub._2, pred._2, obj._2, null.asInstanceOf[Long], null.asInstanceOf[Long], null)
      }
    }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField, tripleTimeStartField, tripleMBRField, triplePropertiesStrField)
    case _ => throw new Exception("Triples parsing setting not found")
  }}, new BaseOperatorParams() {
    override def operationName: Option[String] = Some("Load triples dataset")
  })

  println("Triples dataset: " + triplesPath)
}