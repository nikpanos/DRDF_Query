package gr.unipi.datacron.store

import gr.unipi.datacron.common._
import gr.unipi.datacron.common.grid.EquiGrid

import collection.mutable.HashMap
import scala.collection.mutable

class SpatialGrid() {
  val spatialGrid = new EquiGrid(AppConfig.getInt(Consts.qfpSpatialBits),
    Array(AppConfig.getDouble(Consts.qfpUniverseLatLower), AppConfig.getDouble(Consts.qfpUniverseLonLower)),
    Array(AppConfig.getDouble(Consts.qfpUniverseLatUpper), AppConfig.getDouble(Consts.qfpUniverseLonUpper)))
  
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