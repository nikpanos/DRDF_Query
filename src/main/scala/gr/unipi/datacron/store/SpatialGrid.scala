package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.common.grid.variant2D.EquiGrid

class SpatialGrid() {
  val spatialGrid = new EquiGrid(DataStore.dictionaryRedis.getDynamicSetting(redisKeyEncodingBitsSpatial).get.toInt,
    Array(DataStore.dictionaryRedis.getDynamicSetting(redisKeyEncodingLatLower).get.toDouble, DataStore.dictionaryRedis.getDynamicSetting(redisKeyEncodingLonLower).get.toDouble),
    Array(DataStore.dictionaryRedis.getDynamicSetting(redisKeyEncodingLatUpper).get.toDouble, DataStore.dictionaryRedis.getDynamicSetting(redisKeyEncodingLonUpper).get.toDouble))
  
  def getSpatialIds(c: SpatioTemporalRange): Map[Long, Boolean] = {
    val lo = (java.lang.Double.valueOf(c.low.latitude), java.lang.Double.valueOf(c.low.longitude))
    val hi = (java.lang.Double.valueOf(c.high.latitude), java.lang.Double.valueOf(c.high.longitude))
    import scala.collection.JavaConverters._

    spatialGrid.getGridCells(lo, hi).asScala.map(x => {(x._1.longValue(), x._2.booleanValue())}).toMap
  }
}