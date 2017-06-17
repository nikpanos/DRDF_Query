package gr.unipi.datacron.store

import java.util

import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.io.Source

class TemporalGrid() {
  import DataStore.spark.implicits._

  private val intervalsPath: String = if (AppConfig.yarnMode) {
    AppConfig.getString(qfpNamenode) + AppConfig.getString(Consts.qfpHdfsPrefix) + AppConfig.getString(Consts.qfpIntrvlsPath)
  }
  else {
    AppConfig.getString(Consts.qfpIntrvlsPath)
  }

  private val timeIntervalsStr: Array[String] = Benchmarks.doBenchmark[Array[String]](() => {
    DataStore.spark.read.text(intervalsPath).map(_.getAs[String](0)).collect()
  }, new BaseOperatorParams() {
    override def operationName: Option[String] = Some("Load time intervals dataset")
  })

  val timeIntervals: Array[Long] = timeIntervalsStr.map(_.toLong)

  println("Time intervals dataset: " + intervalsPath)

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