package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.grid.EquiGrid
import collection.mutable.HashMap

class SpatialGrid(config: Config, expData: ExpData) {
  val spatialGrid = new EquiGrid(config.getInt(Consts.qfpSpatialBits), Consts.universeLowCorner,  Consts.universeUpperCorner);
  
  def getSpatialIds(c: SpatioTemporalConstraints): HashMap[Long, Boolean] = {
    val lo = (java.lang.Double.valueOf(c.low.latitude), java.lang.Double.valueOf(c.low.longitude))
    val hi = (java.lang.Double.valueOf(c.high.latitude), java.lang.Double.valueOf(c.high.longitude))
    val it = spatialGrid.getGridCells(lo, hi).entrySet().iterator()
    var result = new HashMap[Long, Boolean]
    while (it.hasNext) {
      var pair = it.next()
      result += (pair.getKey.longValue() -> pair.getValue.toString().toBoolean)
    }
    return result
  }
}