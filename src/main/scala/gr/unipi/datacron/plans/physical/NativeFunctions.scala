package gr.unipi.datacron.plans.physical

import java.text.SimpleDateFormat

import gr.unipi.datacron.plans.physical.traits.filterBySpatioTemporalRangeParams
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.plans.logical.dynamicPlans.functions.RegisteredFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, udf}

object NativeFunctions {

  private def doFilter(wktColumn: String, timeColumn: String, altitudeColumn: Option[String], range: SpatioTemporalRange, pruneKeyColumn: Option[String]): Column = {
    //val wktColumn = params.spatioTemporalColums(0)
    //val timeColumn = params.spatioTemporalColums(1)
    //val altitudeColumn = if (params.spatioTemporalColums.length > 2) Some(params.spatioTemporalColums(2)) else None

    val lowerCorner = range.low
    val upperCorner = range.high

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

    if (pruneKeyColumn.isDefined) {
      val pruneKey = pruneKeyColumn.get
      if (altitudeColumn.isDefined) {
        filterBy3D(col(pruneKey), col(wktColumn), col(altitudeColumn.get), col(timeColumn))
      }
      else {
        filterBy2D(col(pruneKey), col(wktColumn), col(timeColumn))
      }
    }
    else {
      if (altitudeColumn.isDefined) {
        filterBy3D_noKey(col(wktColumn), col(altitudeColumn.get), col(timeColumn))
      }
      else {
        filterBy2D_noKey(col(wktColumn), col(timeColumn))
      }
    }
  }

  /*def filterBySpatioTemporalBox2D(): RegisteredFunction = {
    def applyFilter(cols: Array[Column]): Column = {
      cols(0)
    }

    RegisteredFunction(functionSpatioTemporalBox, -1, false, applyFilter)
  }*/

}
