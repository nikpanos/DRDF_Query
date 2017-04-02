package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import scala.io.Source
import java.util.Arrays

class TemporalGrid(config: Config) {
  val timeIntervals = Source.fromFile(config.getString(Consts.qfpIntrvlsPath)).getLines().map(_.toLong).toArray
  
  def getIntervalId(x: Long): Int = {
    var result = Arrays.binarySearch(timeIntervals, x)
    if (result > 0) {
      result = result - 1;
    }
    else {
      result = math.abs(result) - 2
    }
    return result
  }
  
  def getIntervalIds(c: SpatioTemporalRange): (Int, Int) = (getIntervalId(c.low.time), getIntervalId(c.high.time))
}