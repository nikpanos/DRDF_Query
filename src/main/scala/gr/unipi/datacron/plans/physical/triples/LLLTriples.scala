package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

case class LLLTriples() extends BaseTriples {
  override def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame = {
    if (AppConfig.getBoolean(qfpEnableFilterByEncodedInfo)) {
      //println("filtering by subject info")
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
        withColumn(triplePruneSubKeyField, getPruneKey(col(tripleSubLongField))).filter(col(tripleSubLongField) > -1)
    }
    else {
      params.df.withColumn(triplePruneSubKeyField, lit(3))
    }
  }

  private def getFilter(df: DataFrame, fil: (String, String)): Column = {
    df(fil._1) === fil._2
  }

  override def filterByMultipleOr(params: filterByMultipleOrParams): DataFrame = {
    val df = params.df
    val filters = params.colNamesAndValues.tail.foldLeft(getFilter(df, params.colNamesAndValues.head))((c: Column, fil: (String, String)) => {
      c.or(getFilter(df, fil))
    })
    println(filters)
    params.df.filter(filters)
  }

  override def unionDataframes(params: unionDataframesParams): DataFrame = params.df1.union(params.df2)
}
