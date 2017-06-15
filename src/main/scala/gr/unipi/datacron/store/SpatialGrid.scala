package gr.unipi.datacron.store

import java.lang

import gr.unipi.datacron.common._
import gr.unipi.datacron.common.grid.EquiGrid

class SpatialGrid() {
  val spatialGrid = new EquiGrid(AppConfig.getInt(Consts.qfpSpatialBits),
    Array(AppConfig.getDouble(Consts.qfpUniverseLatLower), AppConfig.getDouble(Consts.qfpUniverseLonLower)),
    Array(AppConfig.getDouble(Consts.qfpUniverseLatUpper), AppConfig.getDouble(Consts.qfpUniverseLonUpper)))
  
  def getSpatialIds(c: SpatioTemporalRange): Map[Long, Boolean] = {
    val lo = (java.lang.Double.valueOf(c.low.latitude), java.lang.Double.valueOf(c.low.longitude))
    val hi = (java.lang.Double.valueOf(c.high.latitude), java.lang.Double.valueOf(c.high.longitude))
    import scala.collection.JavaConverters._

    spatialGrid.getGridCells(lo, hi).asScala.map(x => {(x._1.longValue(), x._2.booleanValue())}).toMap
  }
}