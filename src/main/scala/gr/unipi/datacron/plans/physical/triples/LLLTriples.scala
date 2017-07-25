package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.common.AppConfig
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

  def filterByPOandKeepSpatioTemporal(params: filterByPOandKeepSpatioTemporalParams): DataFrame = {
    params.df.filter((col(triplePredLongField) === params.pred.get).and(col(tripleObjLongField) === params.obj.get).or(
                      (col(triplePredLongField) === params.predSpatial).or(col(triplePredLongField) === params.predTemporal)))
  }

  override def pointSearchObject(params: pointSearchObjectParams): Option[Long] =
    Try(params.df.filter(col(tripleSubLongField) === params.sub).filter(col(triplePredLongField) === params.pred).
      first().getAs[Long](tripleObjLongField)).toOption


  override def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame = {
    if (AppConfig.getBoolean(qfpEnableFilterByEncodedInfo)) {
      val intervalIds = DataStore.temporalGrid.getIntervalIds(params.constraints)
      val bIntervalIds = DataStore.sc.broadcast(intervalIds)
      val bSpatialIds = DataStore.sc.broadcast(DataStore.spatialGrid.getSpatialIds(params.constraints))
      val bEncoder = DataStore.sc.broadcast(params.encoder)

      val result = if (params.df.hasColumn(tripleSubLongField)) params.df else addSubjectInfo(params.df)

      val getPruneKey = udf((sub: Long) => {
        var key = -1
        if (sub >= 0) {
          val components = bEncoder.value.decodeComponentsFromKey(sub)

          if ((components._1 >= bIntervalIds.value._1) && (components._1 <= bIntervalIds.value._2)) {
            //not pruned by temporal
            val sp = bSpatialIds.value.get(components._2)
            if (sp.nonEmpty) {
              //not pruned by spatial
              key = 3 //initially set to need both refinements
              if ((components._1 > bIntervalIds.value._1) && (components._1 < bIntervalIds.value._2)) {
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

      val lowerId = params.encoder.encodeKeyFromComponents(intervalIds._1, 0, 0)
      val higherId = params.encoder.encodeKeyFromComponents(intervalIds._2 + 1, 0, 0)

      result.filter(col(tripleSubLongField) >= lowerId).filter(col(tripleSubLongField) < higherId).
        withColumn(triplePruneSubKeyField, getPruneKey(col(tripleSubLongField))).filter(col(triplePruneSubKeyField) > -1)
    }
    else {
      params.df.withColumn(triplePruneSubKeyField, lit(3))
    }
  }
}
