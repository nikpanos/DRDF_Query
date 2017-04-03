package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.Consts
import org.apache.spark.sql.DataFrame

private[store] class TriplesData(config: Config) {
  import DataStore.spark.implicits._
  
  val data: DataFrame = config.getString(qfpParseTriples) match {
    case Consts.parseString => DataStore.spark.read.text(config.getString(qfpTriplesPath)).toDF(tripleSpoStrField)
    case _ => throw new Exception("Triples parsing setting not found")
  }
}