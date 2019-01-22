package gr.unipi.datacron.plans.physical.triples

import java.text.SimpleDateFormat

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.DataFrameUtils._
import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

case class LLLTriples() extends BasePhysicalPlan with TTriples {

  //prunkey values:
  // -1: should be pruned (either negative id, or out of range)
  // 0: no refinement needed (definitely in the result set)
  // 1: Needs only temporal refinement (Does not need spatial refinement)
  // 2: Needs only spatial refinement (Does not need temporal refinement)
  // 3: Needs both refinements
  override def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame = {
    val intervalIds = DataStore.temporalGrid.getIntervalIds(params.constraints)
    //println(intervalIds)
    //println(params.constraints)
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
            key = 3 //initially set to need both refinements
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
    val wktColumn = params.spatioTemporalColums(0)
    val timeColumn = params.spatioTemporalColums(1)
    val altitudeColumn = if (params.spatioTemporalColums.length > 2) Some(params.spatioTemporalColums(2)) else None

    val lowerCorner = params.range.low
    val upperCorner = params.range.high

    val dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
    val formatter = new SimpleDateFormat(dateFormat)

    val filterBy = (pruneKey: Int, decodedSpatial: String, decodedAltitude: Option[String], decodedTemporal: String) => {
      var tmpResult = ((pruneKey >> 0) & 1) != 1
      var sptResult = ((pruneKey >> 1) & 1) != 1

      if (!tmpResult) {
        val d = formatter.parse(decodedTemporal).getTime
        tmpResult = (d >= lowerCorner.time) && (d <= upperCorner.time)
      }

      if (tmpResult && !sptResult) {
        //refine spatial
        val pos = decodedSpatial.lastIndexOf(lonLatSeparator)
        val lon = decodedSpatial.substring(7, pos).toDouble
        val lat = decodedSpatial.substring(pos + 1, decodedSpatial.length - 1).toDouble
        if (decodedAltitude.isDefined) {
          val alt = decodedAltitude.get.toDouble
          sptResult = (lon >= lowerCorner.longitude) && (lon <= upperCorner.longitude) &&
            (lat >= lowerCorner.latitude) && (lat <= upperCorner.latitude) &&
            (alt >= lowerCorner.altitude.get) && (alt <= upperCorner.altitude.get)
        }
        else {
          sptResult = (lon >= lowerCorner.longitude) && (lon <= upperCorner.longitude) &&
            (lat >= lowerCorner.latitude) && (lat <= upperCorner.latitude)
        }
      }

      tmpResult && sptResult
    }

    val filterBy3D = udf((pruneKey: Int, decodedSpatial: String, decodedAltitude: String, decodedTemporal: String) => {
      filterBy(pruneKey, decodedSpatial, Some(decodedAltitude), decodedTemporal)
    })

    val filterBy2D = udf((pruneKey: Int, decodedSpatial: String, decodedTemporal: String) => {
      filterBy(pruneKey, decodedSpatial, None, decodedTemporal)
    })

    val filterBy3D_noKey = udf((decodedSpatial: String, decodedAltitude: String, decodedTemporal: String) => {
      filterBy(3, decodedSpatial, Some(decodedAltitude), decodedTemporal)
    })

    val filterBy2D_noKey = udf((decodedSpatial: String, decodedTemporal: String) => {
      filterBy(3, decodedSpatial, None, decodedTemporal)
    })

    if (AppConfig.getOptionalBoolean(qfpEnableFilterByEncodedInfo).getOrElse(true)) {
      val pruneKey = params.df.findColumnNameWithPrefix(triplePruneSubKeyField).get
      if (altitudeColumn.isDefined) {
        params.df
          //.filter(unix_timestamp(params.df(sanitize(params.temporalColumn)), dateFormat).between(lower, upper))
          .filter(filterBy3D(col(pruneKey), col(wktColumn), col(altitudeColumn.get), col(timeColumn)))
      }
      else {
        params.df
          //.filter(unix_timestamp(params.df(sanitize(params.temporalColumn)), dateFormat).between(lower, upper))
          .filter(filterBy2D(col(pruneKey), col(wktColumn), col(timeColumn)))
      }
    }
    else {
      if (altitudeColumn.isDefined) {
        params.df
          //.filter(unix_timestamp(params.df(sanitize(params.temporalColumn)), dateFormat).between(lower, upper))
          .filter(filterBy3D_noKey(col(wktColumn), col(altitudeColumn.get), col(timeColumn)))
      }
      else {
        params.df
          //.filter(unix_timestamp(params.df(sanitize(params.temporalColumn)), dateFormat).between(lower, upper))
          .filter(filterBy2D_noKey(col(wktColumn), col(timeColumn)))
      }
    }
  }

  /*override def filterBySpatioTemporalRange(params: filterBySpatioTemporalRangeParams): DataFrame = {
    val wktColumn = params.spatioTemporalColums(0)
    val timeColumn = params.spatioTemporalColums(1)
    val altitudeColumn = if (params.spatioTemporalColums.length > 2) Some(params.spatioTemporalColums(2)) else None

    val lowerCorner = params.range.low
    val upperCorner = params.range.high

    val dateFormat = "yyyy-MM-dd'T'HH:mm:ss"
    val formatter = new SimpleDateFormat(dateFormat)

    val filterBy = (pruneKey: Int, decodedSpatial: String, decodedAltitude: Option[String]) => {
      var sptResult = ((pruneKey >> 1) & 1) != 1

      if (!sptResult) {
        //refine spatial
        val decodedObject = decodedSpatial.substring(7, decodedSpatial.length - 1)
        val lonlat = decodedObject.split(lonLatSeparator)
        val lon = lonlat(0).toDouble
        val lat = lonlat(1).toDouble
        if (decodedAltitude.isDefined) {
          val alt = decodedAltitude.get.toDouble
          sptResult = (lon >= lowerCorner.longitude) && (lon <= upperCorner.longitude) &&
            (lat >= lowerCorner.latitude) && (lat <= upperCorner.latitude) &&
            (alt >= lowerCorner.altitude.get) && (alt <= upperCorner.altitude.get)
        }
        else {
          sptResult = (lon >= lowerCorner.longitude) && (lon <= upperCorner.longitude) &&
            (lat >= lowerCorner.latitude) && (lat <= upperCorner.latitude)
        }
      }

      sptResult
    }

    val filterBy3D = udf((pruneKey: Int, decodedSpatial: String, decodedAltitude: String) => {
      filterBy(pruneKey, decodedSpatial, Some(decodedAltitude))
    })

    val filterBy2D = udf((pruneKey: Int, decodedSpatial: String) => {
      filterBy(pruneKey, decodedSpatial, None)
    })

    val pruneKey = params.df.findColumnNameWithPrefix(triplePruneSubKeyField).get

    val df1 = params.df
      .filter(unix_timestamp(params.df(sanitize(timeColumn)), dateFormat).between(lowerCorner.time / 1000, upperCorner.time / 1000))

    if (altitudeColumn.isDefined) {
      df1.filter(filterBy2D(col(sanitize(pruneKey)), col(sanitize(wktColumn)), col(sanitize(altitudeColumn.get))))
    }
    else {
      df1.filter(filterBy2D(col(sanitize(pruneKey)), col(sanitize(wktColumn))))
    }
  }*/

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

  override def limitResults(params: limitResultsParams): DataFrame = params.df.limit(params.limitNo)

  override def sortResults(params: sortResultsParams): DataFrame = {
    val sortExprs = params.cols.map(x => {
      val colName = x._1
      if (x._2) asc(colName)
      else desc(colName)
    })
    params.df.orderBy(sortExprs: _*)
  }

  override def filterByValue(params: filterByValueParams): DataFrame = params.df.filter(params.value)
}
