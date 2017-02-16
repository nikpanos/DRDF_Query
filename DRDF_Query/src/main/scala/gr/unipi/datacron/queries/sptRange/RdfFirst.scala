package gr.unipi.datacron.queries.sptRange

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._
import gr.unipi.datacron.queries.operators.RdfOperators
import gr.unipi.datacron.queries.operators.CompositeKeyOperators
import gr.unipi.datacron.store.ExpData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
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
  
  private def refineResult(qDetails: SpatioTemporalConstraints, data: DataFrame, soForTemporalRefinement: scala.collection.Map[Long, String],
      soForSpatialRefinement: scala.collection.Map[Long, String]): DataFrame = {
    import ExpData.spark.implicits._
    
    println("Executing rdf-first spatio-temporal range query")
    
    val bcTemporal = ExpData.sc.broadcast(soForTemporalRefinement)
    val bcSpatial = ExpData.sc.broadcast(soForSpatialRefinement)
    
    val result = data.rdd.map(x => {
      var tmpResult = ((x.getAs[Int]("pruneKey") >> 0) & 1) != 1
      var sptResult = ((x.getAs[Int]("pruneKey") >> 1) & 1) != 1
      
      if (!tmpResult) {
        //refine temporal
        val decodedObject = bcTemporal.value.apply(x.getAs[Long]("subject")).toLong
        if ((decodedObject >= qDetails.low.time) && (decodedObject <= qDetails.high.time)) {
          tmpResult = true
        }
      }
      
      if (!sptResult) {
        //refine spatial
        //val sp = kv._2.substring(0, kv._2.indexOf(Consts.tripleFieldsSeperator) + 1) + encodedUriMBR
        //val encodedObject = data.triples.getObjectBySP(sp)
        //val decodedObject = data.dictionary.getValueById(encodedObject).substring(6)
        val decodedObject = bcSpatial.value.apply(x.getAs[Long]("subject")).substring(6)
        val lonlat = decodedObject.substring(0, decodedObject.length - 1).split(Consts.lonLatSeparator)
        val lon = lonlat(0).toDouble
        val lat = lonlat(1).toDouble
        if ((lon >= qDetails.low.longitude) && (lon <= qDetails.high.longitude) &&
            (lat >= qDetails.low.latitude) && (lat <= qDetails.high.latitude)) {
          sptResult = true
        }
        
      }
      
      (tmpResult && sptResult, x.getAs[String]("spo"))
    }).toDF("result", "spo")
    return result.filter($"result")
  }

  override def executeQuery(): Boolean = {
    import ExpData.spark.implicits._
    
    val filteredSPO = RdfOperators.simpleFilter(ExpData.triplesData, tripleFilter)
    
    val filteredByIdInfo = CompositeKeyOperators.filterBySpatiotemporalInfo(filteredSPO, constraints, encoder)
    //println(filteredByIdInfo.count)
    
    val encodedUriMBR = ExpData.dictionary.getKeyByValue(Consts.uriMBR).toLong
    val encodedUriTime = ExpData.dictionary.getKeyByValue(Consts.uriTime).toLong
    
    val flt = ((bit: Int) => {
      udf {(x: Int) => {
        ((x >> bit) & 1) == 1
      }
    }})
    
    val idsForTemporalRefinement = filteredByIdInfo.filter(flt(0)($"pruneKey")).select($"subject").map(r => (r(0).asInstanceOf[Long], encodedUriTime)).collect()
    val idsForSpatialRefinement = filteredByIdInfo.filter(flt(1)($"pruneKey")).select($"subject").map(r => (r(0).asInstanceOf[Long], encodedUriMBR)).collect()
    
    val soForTemporalRefinementEncoded = ExpData.triples.getListOByListSP(idsForTemporalRefinement)
    //println(soForTemporalRefinementEncoded.size)
    val soForSpatialRefinementEncoded = ExpData.triples.getListOByListSP(idsForSpatialRefinement)
    //println(soForSpatialRefinementEncoded.size)
    
    val soForTemporalRefinementDecoded = ExpData.dictionary.getValuesListByKeysList(soForTemporalRefinementEncoded)
    //println(soForTemporalRefinementDecoded.size)
    val soForSpatialRefinementDecoded = ExpData.dictionary.getValuesListByKeysList(soForSpatialRefinementEncoded)
    //println(soForSpatialRefinementDecoded.size)
    
    val result = refineResult(constraints, filteredByIdInfo, soForTemporalRefinementDecoded, soForSpatialRefinementDecoded)
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