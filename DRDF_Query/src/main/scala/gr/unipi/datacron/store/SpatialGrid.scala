package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.grid.EquiGrid

import collection.mutable.HashMap
import scala.collection.mutable

class SpatialGrid(config: Config) {
  val spatialGrid = new EquiGrid(config.getInt(Consts.qfpSpatialBits),
    Array(config.getDouble(Consts.qfpUniverseLatLower), config.getDouble(Consts.qfpUniverseLonLower)),
    Array(config.getDouble(Consts.qfpUniverseLatUpper), config.getDouble(Consts.qfpUniverseLonUpper)))
  
  def getSpatialIds(c: SpatioTemporalRange): mutable.HashMap[Long, Boolean] = {
    val lo = (java.lang.Double.valueOf(c.low.latitude), java.lang.Double.valueOf(c.low.longitude))
    val hi = (java.lang.Double.valueOf(c.high.latitude), java.lang.Double.valueOf(c.high.longitude))
    val it = spatialGrid.getGridCells(lo, hi).entrySet().iterator()
    var result = new mutable.HashMap[Long, Boolean]
    while (it.hasNext) {
      var pair = it.next()
      result += (pair.getKey.longValue() -> pair.getValue.toString.toBoolean)
    }
    result
  }
}