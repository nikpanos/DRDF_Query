package gr.unipi.datacron.queries

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.RegexUtils._
import org.apache.spark.RangePartitioner
import gr.unipi.datacron.common.ArrayUtils._

class StrQuery(config: Config) extends BaseQuery(config) {

  val latLower: Double = config.getDouble(Consts.qfpLatLower)
  val lonLower: Double = config.getDouble(Consts.qfpLonLower)

  val latUpper: Double = config.getDouble(Consts.qfpLatUpper)
  val lonUpper: Double = config.getDouble(Consts.qfpLonUpper)

  val timeLower: Long = config.getLong(Consts.qfpTimeLower)
  val timeUpper: Long = config.getLong(Consts.qfpTimeUpper)

  val triple = new SPO(
    config.getString(Consts.qfpTripleS),
    config.getString(Consts.qfpTripleP),
    config.getString(Consts.qfpTripleO))

  override def executeQuery(): Boolean = {
    val searchStr = triple.getRegExpString
    val filteredSPO = data.triples.filter(searchStr.matches(_))
    println(filteredSPO.count)
    
    val intervalIds = data.getIntervalIds(timeLower, timeUpper)
    println(intervalIds)
    
    val spatialIds = data.getSpatialIds((latLower, lonLower), (latUpper, lonUpper))
    println(spatialIds.size)
    println(spatialIds)

    //val sorted = data.intervals.map(_.split("\\t")).map(x => (x(0).toInt, x(1).toLong)).glom().cache()
    //for ((k, v) <- sorted.map(a => a.slice(a.binarySearch(timeLower), a.binarySearch(timeUpper) + 1)).collect().flatten) println(s"$k, $v")

    /*val p: RangePartitioner[Int, Int] = sorted.partitioner.get.asInstanceOf[RangePartitioner[Int, Int]];
    val (lower, upper) = (10, 20)
    val range = p.getPartition(lower) to p.getPartition(upper)
    println(range)
    val rangeFilter = (i: Int, iter: Iterator[(Int, Int)]) => {
      if (range.contains(i))
        for ((k, v) <- iter if k >= lower && k <= upper) yield (k, v)
      else
        Iterator.empty
    }
    for ((k, v) <- sorted.mapPartitionsWithIndex(rangeFilter, preservesPartitioning = true).collect()) println(s"$k, $v")*/

    true
  }
}