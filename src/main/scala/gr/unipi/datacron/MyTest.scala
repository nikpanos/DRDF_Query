package gr.unipi.datacron

import gr.unipi.datacron.common.{SpatioTemporalInfo, SpatioTemporalRange}
import gr.unipi.datacron.common.grid.variant3D.{EquiGrid, SpatioTemporalObj}

object MyTest {

  def main(args : Array[String]): Unit = {
    val spatialGrid = new EquiGrid(9, Array(30d, -12d, 0d), Array(52d, 37d, 120000d), 3, 2)

    def getSpatialIds(sptLow: SpatioTemporalObj, sptHigh: SpatioTemporalObj): Map[Long, Boolean] = {
      //val lo = new SpatioTemporalObj(java.lang.Double.valueOf(c.low.longitude), java.lang.Double.valueOf(c.low.latitude), 0d, 0L )
      //val hi = new SpatioTemporalObj(java.lang.Double.valueOf(c.high.longitude), java.lang.Double.valueOf(c.high.latitude), 0d, 0L)
      import scala.collection.JavaConverters._

      spatialGrid.getGridCells(sptLow, sptHigh).asScala.map(x => {(x._1.longValue(), x._2.booleanValue())}).toMap
    }

    val sptLow = new SpatioTemporalObj(-1d, 40d, 10d, 0L)
    val sptHigh = new SpatioTemporalObj(10d, 42d, 32000d, 0L)
    println(getSpatialIds(sptLow, sptHigh))
  }
}
