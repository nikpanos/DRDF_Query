package gr.unipi.datacron.operators.triples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

case class LLLTriples() extends BaseTriples {
  import DataStore.spark.implicits._

  override def filterBySPO(df: DataFrame, sub: Option[Long], pred: Option[Long], obj: Option[Long]): DataFrame = {
    var result = df
    if (sub.isDefined) {
      result = result.filter(col(tripleSubLongField) === sub.get)
    }
    if (pred.isDefined) {
      result = result.filter(col(triplePredLongField) === pred.get)
    }
    if (obj.isDefined) {
      result = result.filter(col(tripleObjLongField) === obj.get)
    }
    result
  }

  override def pointSearchObject(df: DataFrame, sub: Long, pred: Long): Option[Long] =
    Try(df.filter(col(tripleSubLongField) === sub).filter(col(triplePredLongField) === pred).
      first().getAs[Long](tripleObjLongField)).toOption
}
