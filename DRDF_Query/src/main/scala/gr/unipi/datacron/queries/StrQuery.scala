package gr.unipi.datacron.queries

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.RegexUtils._
import org.apache.spark.RangePartitioner
import gr.unipi.datacron.common.ArrayUtils._
import gr.unipi.datacron.encoding._
import org.apache.spark.rdd.RDD

class StrQuery(config: Config) extends BaseQuery(config) {
  
  val qDetails = new QueryInfo(config)
  
  private def filterByIdInfo(rdd: RDD[String]): RDD[((Int, Long), String)] = {
  
    val intervalIds = data.temporalGrid.getIntervalIds(qDetails.qTimeLower, qDetails.qTimeUpper)
    val spatialIds = data.spatialGrid.getSpatialIds((qDetails.qLatLower, qDetails.qLonLower), (qDetails.qLatUpper, qDetails.qLonUpper))
    val encoder = new SimpleEncoder(config)
    
    val result = rdd.map(x => {
      val id = x.substring(0, x.indexOf(Consts.tripleFieldsSeparator)).toLong
      val components = encoder.decodeComponentsFromKey(id)
      //println(components)
      
      //Possible key values:
      // -1: pruned by either temporal or spatial
      //  0: definitely a result triple (does not need refinement)
      //  1: needs only temporal refinement
      //  2: needs only spatial refinement
      //  3: needs both temporal and spatial refinement
      var key = -1
      if ((components._1 >= intervalIds._1) && (components._1 <= intervalIds._2)) {
        //not pruned by temporal
        val sp = spatialIds.get(components._2)
        if (sp.nonEmpty) {
          //not pruned by spatial
          key = 3  //initially set to need both refinements
          if ((components._1 > intervalIds._1) && (components._1 < intervalIds._2)) {
            //does not need temporal refinement
            key -= 1
          }
          if (!sp.get) {
            //does not need spatial refinement
            key -= 2
          }
        }
      }
      
      ((key, id), x)
    }).filter(_._1._1 != -1)
    return result
  }
  
  private def refineResult(qDetails: QueryInfo, rdd: RDD[((Int, Long), String)], soForTemporalRefinement: scala.collection.Map[Long, String],
      soForSpatialRefinement: scala.collection.Map[Long, String]): RDD[(Boolean, String)] = {
    
    val result = rdd.map(kv => {
      var tmpResult = ((kv._1._1 >> 0) & 1) != 1
      var sptResult = ((kv._1._1 >> 1) & 1) != 1
      
      if (!tmpResult) {
        //refine temporal
        val decodedObject = soForTemporalRefinement.apply(kv._1._2).toLong
        if ((decodedObject >= qDetails.qTimeLower) && (decodedObject <= qDetails.qTimeUpper)) {
          tmpResult = true
        }
      }
      
      if (!sptResult) {
        //refine spatial
        //val sp = kv._2.substring(0, kv._2.indexOf(Consts.tripleFieldsSeperator) + 1) + encodedUriMBR
        //val encodedObject = data.triples.getObjectBySP(sp)
        //val decodedObject = data.dictionary.getValueById(encodedObject).substring(6)
        val decodedObject = soForSpatialRefinement.apply(kv._1._2).substring(6)
        val lonlat = decodedObject.substring(0, decodedObject.length - 1).split(Consts.lonLatSeparator)
        val lon = lonlat(0).toDouble
        val lat = lonlat(1).toDouble
        if ((lon >= qDetails.qLonLower) && (lon <= qDetails.qLonUpper) && (lat >= qDetails.qLatLower) && (lat <= qDetails.qLatUpper)) {
          sptResult = true
        }
        
      }
      
      (tmpResult && sptResult, kv._2)
    }).filter(_._1)
    return result
  }

  override def executeQuery(): Boolean = {
    val triple = new SPO(
      config.getString(Consts.qfpTripleS),
      config.getString(Consts.qfpTripleP),
      config.getString(Consts.qfpTripleO))
    
    val searchStr = triple.getRegExpString
    val filteredSPO = data.triplesData.filter(searchStr.matches(_))
    println(filteredSPO.count)
    
    val filteredByIdInfo = filterByIdInfo(filteredSPO)
    println(filteredByIdInfo.count)
    
    val encodedUriMBR = data.dictionary.getIdByValue(Consts.uriMBR)
    val encodedUriTime = data.dictionary.getIdByValue(Consts.uriTime)
    
    val idsForTemporalRefinement = filteredByIdInfo.filter(x => {((x._1._1 >> 0) & 1) == 1}).map(_._1._2).collect().toArray
    val idsForSpatialRefinement = filteredByIdInfo.filter(x => {((x._1._1 >> 1) & 1) == 1}).map(_._1._2).collect().toArray
    java.util.Arrays.sort(idsForTemporalRefinement)
    java.util.Arrays.sort(idsForSpatialRefinement)
    println(idsForTemporalRefinement.length)
    println(idsForSpatialRefinement.length)
    
    val soForTemporalRefinementEncoded = data.triples.getListOByListSOnConstantP(idsForTemporalRefinement, encodedUriTime)
    println(soForTemporalRefinementEncoded.size)
    val soForSpatialRefinementEncoded = data.triples.getListOByListSOnConstantP(idsForSpatialRefinement, encodedUriMBR)
    println(soForSpatialRefinementEncoded.size)
    
    val soForTemporalRefinementDecoded = data.dictionary.getValuesListByIdsList(soForTemporalRefinementEncoded)
    println(soForTemporalRefinementDecoded.size)
    val soForSpatialRefinementDecoded = data.dictionary.getValuesListByIdsList(soForSpatialRefinementEncoded)
    println(soForSpatialRefinementDecoded.size)
    
    val result = refineResult(qDetails, filteredByIdInfo, soForTemporalRefinementDecoded, soForSpatialRefinementDecoded)
    println(result.count)
    
    
    
    
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