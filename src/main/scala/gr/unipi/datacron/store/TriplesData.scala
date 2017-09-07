package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import org.apache.spark.sql.DataFrame

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
      val pos = line.indexOf(tripleFieldsSeparator)
      val pos1 = line.lastIndexOf(tripleFieldsSeparator)
      val sub = line.substring(0, pos).toLong
      val pred = line.substring(pos + 1, pos1).toLong
      val obj = line.substring(pos1 + 1).toLong

      (sub, pred, obj)
      }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField)
    case Consts.parseTripleLongProperties => DataStore.spark.read.text(triplesPath).map(s => {
      val line = s.getString(0)
      val pos = line.indexOf(tripleFieldsSeparator)
      val pos1 = line.indexOf(tripleFieldsSeparator, pos + 1)
      val pos2 = line.indexOf(tripleFieldsSeparator, pos1 + 1)

      val sub = line.substring(0, pos).toLong
      val pred = line.substring(pos + 1, pos1).toLong
      if (pos2 > 0) {
        val obj = line.substring(pos1 + 1, pos2).toLong
        val props = line.substring(pos2 + 1)
        (sub, pred, obj, props)
      }
      else {
        val obj = line.substring(pos1 + 1).toLong
        (sub, pred, obj, null)
      }


      }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField, triplePropertiesStrField)
    case _ => throw new Exception("Triples parsing setting not found")
  }}, new BaseOperatorParams() {
    override def operationName: Option[String] = Some("Load triples dataset")
  })

  println("Triples dataset: " + triplesPath)
}