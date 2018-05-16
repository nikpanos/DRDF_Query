package gr.unipi.datacron

import java.sql.Timestamp
import java.text.SimpleDateFormat

import gr.unipi.datacron.common.{AppConfig, SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.plans.physical.PhysicalPlanner
import gr.unipi.datacron.plans.physical.traits.{decodeColumnsParams, filterBySpatioTemporalRangeParams, filterBySubSpatioTemporalInfoParams}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.functions.{col, udf}

import scala.util.Try

object MyTest {

  val dateFormat = new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss")

  def main(args : Array[String]) {
    AppConfig.init(args(0))
    DataStore.init()

    val encoder = SimpleEncoder()

    val constraints = Try(SpatioTemporalRange(
      SpatioTemporalInfo(AppConfig.getDouble(qfpLatLower), AppConfig.getDouble(qfpLonLower), dateFormat.parse(AppConfig.getString(qfpTimeLower)).getTime),
      SpatioTemporalInfo(AppConfig.getDouble(qfpLatUpper), AppConfig.getDouble(qfpLonUpper), dateFormat.parse(AppConfig.getString(qfpTimeUpper)).getTime))).toOption

    val params = filterBySubSpatioTemporalInfoParams(DataStore.nodeData, constraints.get, encoder)
    var df = PhysicalPlanner.filterBySubSpatioTemporalInfo(params)

    val cols = DataStore.spatialAndTemporalShortcutCols

    df = PhysicalPlanner.decodeColumns(decodeColumnsParams(df, Array(cols(0), cols(1)), true))

    df.show()

    /*val tst = udf((pruneKey: Int, decodedSpatial: String) => {
      val sptResult = ((pruneKey >> 1) & 1) != 1

      if (!sptResult) {
        //refine spatial
        val decodedObject = decodedSpatial.substring(7, decodedSpatial.length - 1)
        val lonlat = decodedObject.split(lonLatSeparator)
        lonlat(0).toDouble
      }
      else {
        0d
      }
    })

    df.withColumn("newCol", tst(col("pruneSubKey"),  col("-4"))).select("pruneSubKey", "-4", "newCol").show(false)
    */
    val params1 = filterBySpatioTemporalRangeParams(df, constraints.get, cols(0), cols(1))
    df = PhysicalPlanner.filterBySpatioTemporalRange(params1).select(cols(0), cols(1))

    df.show(10, false)

    println(df.count)
  }

  /*override def filterBySpatioTemporalRange(params: filterBySpatioTemporalRangeParams): DataFrame = {
    val lower = new Timestamp(params.range.low.time)
    val upper = new Timestamp(params.range.high.time)

    val dateFormat = "YYYY-MM-dd'T'HH:mm:ss"

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
      //.filter(to_utc_timestamp(params.df(sanitize(params.temporalColumn)), dateFormat).between(lower, upper))
      .filter(filterBy(col(sanitize(pruneKey)), col(sanitize(params.spatialColumn))))
  }*/
}
