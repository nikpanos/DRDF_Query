package gr.unipi.datacron

import java.text.SimpleDateFormat
import java.util.Date

import com.redis.cluster._

import scala.io.Source

object MyTest {

  /*private val timeIntervals: Array[Long] = Source.fromFile("C:\\Users\\nikp\\Desktop\\timeintervalsSimple32_8_8.txt").getLines().toArray.map(_.toLong)

  def getIntervalId(x: Long): Int = {
    val result = java.util.Arrays.binarySearch(timeIntervals, x)
    if (result > 0) {
      result - 1
    }
    else {
      math.abs(result) - 2
    }
  }

  def getIntervalIds(l: Long, u: Long): (Int, Int) = (getIntervalId(l), getIntervalId(u) + 1)*/

  def main(args : Array[String]) {
    val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
    /*val lowerTime = format.parse("2016-01-09T00:00:50").getTime
    val upperTime = format.parse("2016-01-02T00:02:58").getTime
    println(lowerTime)

    println(getIntervalIds(lowerTime, upperTime))*/

    val d = format.parse("2016-01-09T00:00:50").getTime
    println(d)
    val d1 = new Date
    d1.setTime(d)
    println(format.format(d1))
  }
}
