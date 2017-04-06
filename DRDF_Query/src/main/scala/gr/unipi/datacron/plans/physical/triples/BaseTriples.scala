package gr.unipi.datacron.plans.physical.triples

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.Consts
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.plans.physical.traits.TTriples
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.physical.BaseOperator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

abstract class BaseTriples extends BaseOperator with TTriples {
  import DataStore.spark.implicits._

  //To be overriden in STriples
  private[triples] def addSubjectInfo(df: DataFrame): DataFrame = df

  //To be overriden in STriples
  private[triples] def addPredicateInfo(df: DataFrame): DataFrame = df

  //To be overriden in STriples
  private[triples] def addObjectInfo(df: DataFrame): DataFrame = df

  def filterBySubSpatioTemporalInfo(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder): DataFrame = {

    val intervalIds = DataStore.temporalGrid.getIntervalIds(constraints)
    val spatialIds = DataStore.spatialGrid.getSpatialIds(constraints)

    val result = if (df.hasColumn(tripleSubLongField)) df else addSubjectInfo(df)

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

    result.withColumn(triplePruneSubKeyField, getPruneKey(col(tripleSubLongField))).filter(col(triplePruneSubKeyField) > -1)
  }

  def filterbySpatioTemporalRange(df: DataFrame, range: SpatioTemporalRange): DataFrame = {

    df.filter(x => {
      var tmpResult = ((x.getAs[Int](triplePruneSubKeyField) >> 0) & 1) != 1
      var sptResult = ((x.getAs[Int](triplePruneSubKeyField) >> 1) & 1) != 1

      if (!tmpResult) {
        //refine temporal
        val decodedObject = x.getAs[String](tripleTimeStartField + tripleTranslateSuffix).toLong
        if ((decodedObject >= range.low.time) && (decodedObject <= range.high.time)) {
          tmpResult = true
        }
      }

      if (!sptResult) {
        //refine spatial
        val decodedObject = x.getAs[String](tripleMBRField + tripleTranslateSuffix).substring(6)
        val lonlat = decodedObject.substring(0, decodedObject.length - 1).split(Consts.lonLatSeparator)
        val lon = lonlat(0).toDouble
        val lat = lonlat(1).toDouble
        if ((lon >= range.low.longitude) && (lon <= range.high.longitude) &&
          (lat >= range.low.latitude) && (lat <= range.high.latitude)) {
          sptResult = true
        }

      }

      tmpResult && sptResult
    })
  }

  def prepareForFinalTranslation(df: DataFrame): DataFrame = {
    var result = df
    if (!df.hasColumn(tripleSubLongField)) {
      result = addSubjectInfo(result)
    }
    if (!df.hasColumn(triplePredLongField)) {
      result = addPredicateInfo(result)
    }
    if (!df.hasColumn(tripleObjLongField)) {
      result = addObjectInfo(result)
    }
    result
  }
}
