package gr.unipi.datacron.store

import java.util

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._

import scala.io.Source

class TemporalGrid(config: Config) {
  import DataStore.spark.implicits._

  private val timeIntervalsStr: Array[String] = if (config.getString(qfpSparkMaster).equals("yarn")) {
    //val hdfs = FileSystem.get(new URI(config.getString(qfpNamenode)), new Configuration())
    //val path = new Path(config.getString(Consts.qfpHdfsPrefix) + config.getString(Consts.qfpIntrvlsPath))
    //val stream = new BufferedReader(new InputStreamReader(hdfs.open(path)))
    //val input = Stream.continually(stream.readLine)
    //Stream.continually(stream.readLine).toIterator

    DataStore.spark.read.text(config.getString(Consts.qfpHdfsPrefix) + config.getString(Consts.qfpIntrvlsPath)).map(_.getAs[String](0)).collect()
  }
  else {
    Source.fromFile(config.getString(Consts.qfpIntrvlsPath)).getLines().toArray
  }

  val timeIntervals: Array[Long] = timeIntervalsStr.map(_.toLong)
  
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