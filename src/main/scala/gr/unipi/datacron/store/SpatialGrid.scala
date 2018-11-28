package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.common.grid.variant3D.{EquiGrid, SpatioTemporalObj}

class SpatialGrid() {

  private val dicRedis = DataStore.dictionaryRedis

  private val dimensions = dicRedis.getDynamicSetting(redisKeyDimensions).get.toInt

  private val spatialGrid = if (dimensions == 3) {
    new EquiGrid(dicRedis.getDynamicSetting(redisKeyEncodingBitsSpatial).get.toInt,
      Array(dicRedis.getDynamicSetting(redisKeyEncodingLatLower).get.toDouble,
        dicRedis.getDynamicSetting(redisKeyEncodingLonLower).get.toDouble,
        dicRedis.getDynamicSetting(redisKeyEncodingAltLower).get.toDouble),
      Array(dicRedis.getDynamicSetting(redisKeyEncodingLatUpper).get.toDouble,
        dicRedis.getDynamicSetting(redisKeyEncodingLonUpper).get.toDouble,
        dicRedis.getDynamicSetting(redisKeyEncodingAltUpper).get.toDouble),
      dimensions,
      dicRedis.getDynamicSetting(redisKeyCurveType).get.toInt)
  }
  else {
    new EquiGrid(dicRedis.getDynamicSetting(redisKeyEncodingBitsSpatial).get.toInt,
      Array(dicRedis.getDynamicSetting(redisKeyEncodingLatLower).get.toDouble,
        dicRedis.getDynamicSetting(redisKeyEncodingLonLower).get.toDouble),
      Array(dicRedis.getDynamicSetting(redisKeyEncodingLatUpper).get.toDouble,
        dicRedis.getDynamicSetting(redisKeyEncodingLonUpper).get.toDouble),
      dimensions,
      dicRedis.getDynamicSetting(redisKeyCurveType).get.toInt)
  }

  def getSpatialIds(c: SpatioTemporalRange): Map[Long, Boolean] = {
    val lo = new SpatioTemporalObj(java.lang.Double.valueOf(c.low.longitude), java.lang.Double.valueOf(c.low.latitude), java.lang.Double.valueOf(c.low.altitude.getOrElse(0d)), 0L)
    val hi = new SpatioTemporalObj(java.lang.Double.valueOf(c.high.longitude), java.lang.Double.valueOf(c.high.latitude), java.lang.Double.valueOf(c.high.altitude.getOrElse(0d)), 0L)
    import scala.collection.JavaConverters._

    spatialGrid.getGridCells(lo, hi).asScala.map(x => {
      (x._1.longValue(), x._2.booleanValue())
    }).toMap
  }
}