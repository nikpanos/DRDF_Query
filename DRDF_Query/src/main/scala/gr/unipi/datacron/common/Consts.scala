package gr.unipi.datacron.common

object Consts {
  //Query file params
  val qfpQueryName: String = "name"
  val qfpLatLower: String = "lat_lower"
  val qfpLonLower: String = "lon_lower"
  val qfpLatUpper: String = "lat_upper"
  val qfpLonUpper: String = "lon_upper"
  val qfpTimeLower: String = "time_lower"
  val qfpTimeUpper: String = "time_upper"
  val qfpTripleS: String = "triple_s"
  val qfpTripleP: String = "triple_p"
  val qfpTripleO: String = "triple_o"
  val qfpTotalBits: String = "nTotalBits"
  val qfpSpatialBits: String = "nSpatialBits"
  val qfpIDsBits: String = "nIDsBits"
  val qfpDicPath: String = "dictionary_path"
  val qfpTriplesPath: String = "triples_path"
  val qfpIntrvlsPath: String = "intervals_path"
  val qfpSparkMaster: String = "spark_master"
  
  //universe values
  val universeLowCorner = Array(39.82,12.21);
  val universeUpperCorner = Array(45.82,19.6);
}