package gr.unipi.datacron

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.SpatioTemporalRange
import gr.unipi.datacron.common.grid.variant3D.{EquiGrid, SpatioTemporalObj}

class SpatialGrid1 {

  private val dimensions = 3

  private val spatialGrid = new EquiGrid(9,
      Array(29.0729, -11.4155, 0d),
      Array(77.8401, 39.8693, 0d/*36911.28*/),
      dimensions,
    2)

  def getSpatialIds(c: SpatioTemporalRange): Map[Long, Boolean] = {
    val lo = new SpatioTemporalObj(java.lang.Double.valueOf(c.low.longitude), java.lang.Double.valueOf(c.low.latitude), java.lang.Double.valueOf(c.low.altitude.getOrElse(0d)), 0L)
    val hi = new SpatioTemporalObj(java.lang.Double.valueOf(c.high.longitude), java.lang.Double.valueOf(c.high.latitude), java.lang.Double.valueOf(c.high.altitude.getOrElse(0d)), 0L)
    import scala.collection.JavaConverters._

    spatialGrid.getGridCells(lo, hi).asScala.map(x => {(x._1.longValue(), x._2.booleanValue())}).toMap
  }
}
