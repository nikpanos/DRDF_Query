package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.io.Source
import java.util.Arrays
import gr.unipi.datacron.common.grid.EquiGrid
import collection.mutable.HashMap
import collection.JavaConverters._

class HdfsData(config: Config) {
  val conf = new SparkConf().setAppName(config.getString(Consts.qfpQueryName))
  conf.setMaster(config.getString(Consts.qfpSparkMaster)) //This may be removed later
  val sc = new SparkContext(conf)

  val dictionary = sc.textFile(config.getString(Consts.qfpDicPath))
  //val intervals = sc.textFile(config.getString(Consts.qfpIntrvlsPath))
  val triples = sc.textFile(config.getString(Consts.qfpTriplesPath))
  val spatialGrid = new EquiGrid(config.getInt(Consts.qfpSpatialBits), Consts.universeLowCorner,  Consts.universeUpperCorner);
  
  val timeIntervals = Source.fromFile(config.getString(Consts.qfpIntrvlsPath)).getLines().map(_.toLong).toArray
  
  def getIntervalId(x: Long): Int = {
    var result = Arrays.binarySearch(timeIntervals, x)
    if (result > 0) {
      result = result - 1;
    }
    else {
      result = Math.abs(result) - 2
    }
    return result
  }
  
  def getIntervalIds(low: Long, high: Long): (Int, Int) = (getIntervalId(low), getIntervalId(high))
  
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