package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import org.apache.spark.sql.DataFrame

private[store] class TriplesData() {
  import DataStore.spark.implicits._

  val triplesPath: String = if (AppConfig.yarnMode) {
    AppConfig.getString(qfpNamenode) + AppConfig.getString(qfpHdfsPrefix) + AppConfig.getString(qfpTriplesPath)
  }
  else {
    AppConfig.getString(qfpTriplesPath)
  }
  
  val data: DataFrame = (AppConfig.getString(qfpParseTriples) match {
    case Consts.parseString => DataStore.spark.read.text(triplesPath).toDF(tripleSpoStrField)
    case Consts.parseTripleLong => DataStore.spark.read.text(triplesPath).map(s => {
      val line = s.getAs[String](0)
      val pos = line.indexOf(tripleFieldsSeparator)
      val pos1 = line.lastIndexOf(tripleFieldsSeparator)
      val sub = line.substring(0, pos).toLong
      val pred = line.substring(pos + 1, pos1).toLong
      val obj = line.substring(pos1 + 1).toLong

      (sub, pred, obj)
    }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField)
    case _ => throw new Exception("Triples parsing setting not found")
  }).cache

  println("Triples dataset: " + triplesPath)
}