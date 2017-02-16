package gr.unipi.datacron.common

object Consts {
  //Query file params
  val qfpQueryName = "name"
  val qfpLatLower = "lat_lower"
  val qfpLonLower = "lon_lower"
  val qfpLatUpper = "lat_upper"
  val qfpLonUpper = "lon_upper"
  val qfpTimeLower = "time_lower"
  val qfpTimeUpper = "time_upper"
  val qfpTripleS = "triple_s"
  val qfpTripleP = "triple_p"
  val qfpTripleO = "triple_o"
  val qfpTotalBits = "nTotalBits"
  val qfpSpatialBits = "nSpatialBits"
  val qfpIDsBits = "nIDsBits"
  val qfpDicPath = "dictionary_path"
  val qfpTriplesPath = "triples_path"
  val qfpIntrvlsPath = "intervals_path"
  val qfpSparkMaster = "spark_master"
  val qfpQueryType = "query_type"
  
  //universe values
  val universeLowCorner = Array(39.82,12.21);
  val universeUpperCorner = Array(45.82,19.6);
  
  //field separators
  val tripleFieldsSeparator = " "
  val dicFieldsSeparator = "\t"
  val lonLatSeparator = " "
  
  //RDF URIs
  val uriMBR = ":hasMBR_WKT"
  val uriTime = ":SemanticNodeTimeStart"
  
  //Query types
  val spatialFirstSptRangeQuery = "SpatialFirstSptRangeQuery"
  val rdfFirstSptRangeQuery = "RdfFirstSptRangeQuery"
}