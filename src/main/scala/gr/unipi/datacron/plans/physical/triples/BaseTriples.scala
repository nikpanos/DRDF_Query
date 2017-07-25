package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract private[triples] class BaseTriples extends BasePhysicalPlan with TTriples {
  import DataStore.spark.implicits._

  //To be overriden in STriples
  private[triples] def addSubjectInfo(df: DataFrame): DataFrame = df

  //To be overriden in STriples
  private[triples] def addPredicateInfo(df: DataFrame): DataFrame = df

  //To be overriden in STriples
  private[triples] def addObjectInfo(df: DataFrame): DataFrame = df

  def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame = {

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

  def filterbySpatioTemporalRange(params: filterbySpatioTemporalRangeParams): DataFrame = {

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    params.df.filter(x => {
      var tmpResult = ((x.getAs[Int](triplePruneSubKeyField) >> 0) & 1) != 1
      var sptResult = ((x.getAs[Int](triplePruneSubKeyField) >> 1) & 1) != 1

      if (!tmpResult) {
        //refine temporal
        val decodedObject = format.parse(x.getAs[String](tripleTimeStartField + tripleTranslateSuffix)).getTime
        if ((decodedObject >= params.range.low.time) && (decodedObject <= params.range.high.time)) {
          tmpResult = true
        }
      }

      if (!sptResult) {
        //refine spatial
        val decodedObject = x.getAs[String](tripleMBRField + tripleTranslateSuffix).substring(7)
        val lonlat = decodedObject.substring(0, decodedObject.length - 1).split(lonLatSeparator)
        val lon = lonlat(0).toDouble
        val lat = lonlat(1).toDouble
        if ((lon >= params.range.low.longitude) && (lon <= params.range.high.longitude) &&
          (lat >= params.range.low.latitude) && (lat <= params.range.high.latitude)) {
          sptResult = true
        }

      }

      tmpResult && sptResult
    })
  }

  def prepareForFinalTranslation(params: prepareForFinalTranslationParams): DataFrame = {
    var result = params.df
    if (!params.df.hasColumn(tripleSubLongField)) {
      result = addSubjectInfo(result)
    }
    if (!params.df.hasColumn(triplePredLongField)) {
      result = addPredicateInfo(result)
    }
    if (!params.df.hasColumn(tripleObjLongField)) {
      result = addObjectInfo(result)
    }
    result
  }

  override def filterByColumn(params: filterByColumnParams): DataFrame = {
    params.df.filter(col(params.columnName) === params.value)
  }
}
