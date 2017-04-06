package gr.unipi.datacron.common

object Consts {
  //Query file params
  val qfpQueryName = "name"
  val qfpQueryType = "query_type"

  val qfpLatLower = "spatio_temporal_predicate.lat_lower"
  val qfpLonLower = "spatio_temporal_predicate.lon_lower"
  val qfpLatUpper = "spatio_temporal_predicate.lat_upper"
  val qfpLonUpper = "spatio_temporal_predicate.lon_upper"
  val qfpTimeLower = "spatio_temporal_predicate.time_lower"
  val qfpTimeUpper = "spatio_temporal_predicate.time_upper"

  val qfpTripleS = "star_predicate.s"
  val qfpTripleP = "star_predicate.p"
  val qfpTripleO = "star_predicate.o"

  val qfpTotalBits = "universe.nTotalBits"
  val qfpSpatialBits = "universe.nSpatialBits"
  val qfpIDsBits = "universe.nIDsBits"
  val qfpUniverseLatLower = "universe.lat_lower"
  val qfpUniverseLonLower = "universe.lon_lower"
  val qfpUniverseLatUpper = "universe.lat_upper"
  val qfpUniverseLonUpper = "universe.lon_upper"

  val qfpDicPath = "datasets.dictionary.path"
  val qfpTriplesPath = "datasets.triples.path"
  val qfpIntrvlsPath = "datasets.intervals.path"

  val qfpParseDictionary = "datasets.dictionary.parse"
  val qfpParseTriples = "datasets.triples.parse"

  val qfpSparkMaster = "execution.spark_master"

  val qfpDictionaryTrait = "plans.physical.dictionary"
  val qfpTriples_trait = "plans.physical.triples"
  val qfpJoinTriples_trait = "plans.physical.joinTriples"

  val qfpLogicalPlans = "plans.logical"

  //Query types
  val starSptRangeQuery = "StarSptRange"

  //Logical plans
  val spatialFirstStarSptRangeLPlan = "SpatialFirstStarSptRange"
  val spatialFirstJoinStarSptRangeLPlan = "SpatialFirstJoinStStarSptRange"
  val rdfFirstStarSptRangeLPlan = "RdfFirstStarSptRange"

  //Physical plans
  //TDictionary
  val tLSDictionary = "LSDictionary"
  val tSDictionary = "SDictionary"

  //TTriples
  val tSTriples = "STriples"
  val tLLLTriples = "LLLTriples"

  //TJoinTriples
  val tMBJoinSTriples = "MBJoinSTriples"
  val tMBJoinLLLTriples = "MBJoinLLLTriples"
  
  //field separators
  val tripleFieldsSeparator = " "
  val dicFieldsSeparator = "\t"
  val lonLatSeparator = " "
  
  //RDF URIs
  val uriMBR = ":hasMBR_WKT"
  val uriTime = ":SemanticNodeTimeStart"
  
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
  val parseTripleLong = "LLL"
}

