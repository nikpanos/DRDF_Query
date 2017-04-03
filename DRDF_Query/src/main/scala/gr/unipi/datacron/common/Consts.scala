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
  val qfpDictionaryTrait = "dictionary_trait"
  val qfpTriples_trait = "triples_trait"
  val qfpJoinTriples_trait = "joinTriples_trait"
  val qfpParseDictionary = "parse_dictionary"
  val qfpParseTriples = "parse_triples"

  //Physical plan traits
  //TDictionary
  val tLSDictionary = "LSDictionary"
  val tSDictionary = "SDictionary"

  //TTriples
  val tSTriples = "STriples"

  //TJoinTriples
  val tMBJoinSTriples = "MBJoinSTriples"
  
  //universe values
  val universeLowCorner = Array(39.82, 12.21)
  val universeUpperCorner = Array(45.82, 19.6)
  
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
  
  //Triples fields
  val tripleSpoStrField = "spoStr"
  val triplePruneSubKeyField = "pruneSubKey"
  val tripleSubLongField = "subLong"
  val triplePredLongField = "predLong"
  val tripleObjLongField = "objLong"
  val tripleTranslateSuffix = "_trans"
  val tripleMBRField = "mbr_wkt"
  val tripleTimeStartField = "time_start"
  
  //Dictionary fields
  val dicLineStrField = "lineStr"
  val dicKeyLongField = "keyLong"
  val dicValueStrField = "valueStr"

  //parsing
  val parseString = "S"
  val parseLongString = "LS"
}

