package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.grid.EquiGrid
import collection.mutable.HashMap

class SpatialGrid(config: Config, expData: ExpData) {
  val spatialGrid = new EquiGrid(config.getInt(Consts.qfpSpatialBits), Consts.universeLowCorner,  Consts.universeUpperCorner);
  
  def getSpatialIds(low: (Double, Double), high: (Double, Double)): HashMap[Long, Boolean] = {
    val lo = (java.lang.Double.valueOf(low._1), java.lang.Double.valueOf(low._2))
    val hi = (java.lang.Double.valueOf(high._1), java.lang.Double.valueOf(high._2))
    val it = spatialGrid.getGridCells(lo, hi).entrySet().iterator()
    var result = new HashMap[Long, Boolean]
    while (it.hasNext) {
      var pair = it.next()
      result += (pair.getKey.longValue() -> pair.getValue.toString().toBoolean)
    }
    return result
  }
}