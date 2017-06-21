package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
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


  override def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame = {

    val intervalIds = DataStore.temporalGrid.getIntervalIds(params.constraints)
    val spatialIds = DataStore.spatialGrid.getSpatialIds(params.constraints)

    val result = if (params.df.hasColumn(tripleSubLongField)) params.df else addSubjectInfo(params.df)

    val getPruneKey = udf((sub: Long) => {
      var key = -1
      if (sub >= 0) {
        val components = params.encoder.decodeComponentsFromKey(sub)

        if ((components._1 >= intervalIds._1) && (components._1 <= intervalIds._2)) {
          //not pruned by temporal
          val sp = spatialIds.get(components._2)
          if (sp.nonEmpty) {
            //not pruned by spatial
            key = 3  //initially set to need both refinements
            if ((components._1 > intervalIds._1) && (components._1 < intervalIds._2)) {
              //does not need temporal refinement
              key -= 1
            }
            if (!sp.get) {
              //does not need spatial refinement
              key -= 2
            }
          }
        }
      }
      key
    })

    result.withColumn(triplePruneSubKeyField, getPruneKey(col(tripleSubLongField))).filter(col(triplePruneSubKeyField) > -1)
  }
}
