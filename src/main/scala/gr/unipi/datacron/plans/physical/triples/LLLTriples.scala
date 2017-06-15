package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.traits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.util.Try

case class LLLTriples() extends BaseTriples {
  override def filterByPO(params: filterByPOParams): DataFrame = {
    var result = params.df
    if (params.pred.isDefined) {
      result = result.filter(col(triplePredLongField) === params.pred.get)
    }
    if (params.obj.isDefined) {
      result = result.filter(col(tripleObjLongField) === params.obj.get)
    }
    result
  }

  override def pointSearchObject(params: pointSearchObjectParams): Option[Long] =
    Try(params.df.filter(col(tripleSubLongField) === params.sub).filter(col(triplePredLongField) === params.pred).
      first().getAs[Long](tripleObjLongField)).toOption
}
