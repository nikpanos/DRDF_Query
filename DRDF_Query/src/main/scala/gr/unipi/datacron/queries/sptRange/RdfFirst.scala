package gr.unipi.datacron.queries.sptRange

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import gr.unipi.datacron.queries.operators.RdfOperators
import gr.unipi.datacron.queries.operators.CompositeKeyOperators
import gr.unipi.datacron.store.ExpData
import gr.unipi.datacron.queries.BaseQuery

class RdfFirst(config: Config) extends BaseQuery(config) {
  
  val constraints = new SpatioTemporalConstraints(
      config.getDouble(Consts.qfpLatLower),
      config.getDouble(Consts.qfpLonLower),
      config.getDouble(Consts.qfpLatUpper),
      config.getDouble(Consts.qfpLonUpper),
      config.getLong(Consts.qfpTimeLower),
      config.getLong(Consts.qfpTimeUpper))
  
  val tripleFilter = new SPO(
      config.getString(Consts.qfpTripleS),
      config.getString(Consts.qfpTripleP),
      config.getString(Consts.qfpTripleO))
  
  val encoder = new SimpleEncoder(config)

  override def executeQuery(): Boolean = {
    import ExpData.spark.implicits._

    println("Executing rdf-first spatio-temporal range query")
    
    val filteredSPO = RdfOperators.simpleFilter(ExpData.triplesData, tripleFilter)
    
    val filteredByIdInfo = CompositeKeyOperators.filterBySpatiotemporalInfo(filteredSPO, constraints, encoder)
    //println(filteredByIdInfo.count)
    
    val result = NaiveRefinement.refineResults(filteredByIdInfo, constraints)
    println("Result count: " + result.count)
    
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
