package gr.unipi.datacron.queries.sptRange

import gr.unipi.datacron.common._
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.store.ExpData
import org.apache.spark.sql.functions._

object NaiveRefinement {
  
  private def getResults(qDetails: SpatioTemporalConstraints, data: DataFrame, soForTemporalRefinement: scala.collection.Map[Long, String],
      soForSpatialRefinement: scala.collection.Map[Long, String]): DataFrame = {
    import ExpData.spark.implicits._
    
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
  
  
  def refineResults(data: DataFrame, constraints: SpatioTemporalConstraints): DataFrame = {
    import ExpData.spark.implicits._
    
    val encodedUriMBR = ExpData.dictionary.getKeyByValue(Consts.uriMBR).toLong
    val encodedUriTime = ExpData.dictionary.getKeyByValue(Consts.uriTime).toLong
    
    val flt = ((bit: Int) => {
      udf {(x: Int) => {
        ((x >> bit) & 1) == 1
      }
    }})
    
    val idsForTemporalRefinement = data.filter(flt(0)($"pruneKey")).select($"subject").map(r => (r(0).asInstanceOf[Long], encodedUriTime)).collect()
    val idsForSpatialRefinement = data.filter(flt(1)($"pruneKey")).select($"subject").map(r => (r(0).asInstanceOf[Long], encodedUriMBR)).collect()
    
    val soForTemporalRefinementEncoded = ExpData.triples.getListOByListSP(idsForTemporalRefinement)
    //println(soForTemporalRefinementEncoded.size)
    val soForSpatialRefinementEncoded = ExpData.triples.getListOByListSP(idsForSpatialRefinement)
    //println(soForSpatialRefinementEncoded.size)
    
    val soForTemporalRefinementDecoded = ExpData.dictionary.getValuesListByKeysList(soForTemporalRefinementEncoded)
    //println(soForTemporalRefinementDecoded.size)
    val soForSpatialRefinementDecoded = ExpData.dictionary.getValuesListByKeysList(soForSpatialRefinementEncoded)
    //println(soForSpatialRefinementDecoded.size)
    
    return getResults(constraints, data, soForTemporalRefinementDecoded, soForSpatialRefinementDecoded)
  }
}