package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

case class LLLTriples() extends BasePhysicalPlan with TTriples {
  override def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame = {
    val intervalIds = DataStore.temporalGrid.getIntervalIds(params.constraints)
    //println(intervalIds)
    //println(params.constraints.low)
    val spatialIds = DataStore.spatialGrid.getSpatialIds(params.constraints)
    val encoder = params.encoder
    val result = params.df

    val getPruneKey = udf((sub: Long) => {
      var key = -1
      if (sub >= 0) {
        val components = encoder.decodeComponentsFromKey(sub)

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

    val lowerId = encoder.encodeKeyFromComponents(intervalIds._1, 0, 0)
    val higherId = encoder.encodeKeyFromComponents(intervalIds._2 + 1, 0, 0)

    result.filter(col(tripleSubLongField) >= lowerId).filter(col(tripleSubLongField) < higherId).
      withColumn(triplePruneSubKeyField, getPruneKey(col(tripleSubLongField))).filter(col(triplePruneSubKeyField) > -1)
  }

  override def filterBySpatioTemporalRange(params: filterBySpatioTemporalRangeParams): DataFrame = {
    val lower = params.range.low.time / 1000L
    val upper = params.range.high.time / 1000L

    val dateFormat = "yyyy-MM-dd'T'HH:mm:ss"

    val filterBy = udf((pruneKey: Int, decodedSpatial: String) => {
      val sptResult = ((pruneKey >> 1) & 1) != 1

      if (!sptResult) {
        //refine spatial
        val decodedObject = decodedSpatial.substring(7, decodedSpatial.length - 1)
        val lonlat = decodedObject.split(lonLatSeparator)
        val lon = lonlat(0).toDouble
        val lat = lonlat(1).toDouble
        if ((lon >= params.range.low.longitude) && (lon <= params.range.high.longitude) &&
          (lat >= params.range.low.latitude) && (lat <= params.range.high.latitude)) {
          true
        }
        else {
          false
        }
      }
      else {
        true
      }
    })

    val pruneKey = params.df.findColumnNameWithPrefix(triplePruneSubKeyField).get

    params.df
      .filter(unix_timestamp(params.df(sanitize(params.temporalColumn)), dateFormat).between(lower, upper))
      .filter(filterBy(col(sanitize(pruneKey)), col(sanitize(params.spatialColumn))))
  }

  override def filterByColumn(params: filterByColumnParams): DataFrame = {
    params.df.filter(col(params.columnName) === params.value)
  }

  private def getFilter(df: DataFrame, fil: (String, String)): Column = {
    df(fil._1) === fil._2
  }

  override def filterByMultipleOr(params: filterByMultipleOrParams): DataFrame = {
    val df = params.df
    val filters = params.colNamesAndValues.tail.foldLeft(getFilter(df, params.colNamesAndValues.head))((c: Column, fil: (String, String)) => {
      c.or(getFilter(df, fil))
    })
    params.df.filter(filters)
  }

  override def unionDataframes(params: unionDataframesParams): DataFrame = params.df1.union(params.df2)
}
