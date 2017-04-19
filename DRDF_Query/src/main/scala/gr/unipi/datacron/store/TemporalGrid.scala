package gr.unipi.datacron.store

import java.util

import com.typesafe.config.Config
import gr.unipi.datacron.common._

import scala.io.Source

class TemporalGrid(config: Config) {
  val timeIntervals: Array[Long] = Source.fromFile(config.getString(Consts.qfpIntrvlsPath)).getLines().map(_.toLong).toArray
  
  def getIntervalId(x: Long): Int = {
    val result = util.Arrays.binarySearch(timeIntervals, x)
    if (result > 0) {
      result - 1
    }
    else {
      math.abs(result) - 2
    }
  }
  
  def getIntervalIds(c: SpatioTemporalRange): (Int, Int) = (getIntervalId(c.low.time), getIntervalId(c.high.time) + 1)
}