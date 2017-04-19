package gr.unipi.datacron.common

object Consts {
  //Query file params
  final val qfpQueryName = "name"
  final val qfpQueryType = "query.type"

  final val qfpLatLower = "query.spatio_temporal_predicate.lat_lower"
  final val qfpLonLower = "query.spatio_temporal_predicate.lon_lower"
  final val qfpLatUpper = "query.spatio_temporal_predicate.lat_upper"
  final val qfpLonUpper = "query.spatio_temporal_predicate.lon_upper"
  final val qfpTimeLower = "query.spatio_temporal_predicate.time_lower"
  final val qfpTimeUpper = "query.spatio_temporal_predicate.time_upper"

  final val qfpTripleP = "query.rdf_predicate.condition.p"
  final val qfpTripleO = "query.rdf_predicate.condition.o"

  final val qfpJoinKey = "query.rdf_predicate.join.key"
  final val qfpJoinTripleP = "query.rdf_predicate.join.condition.p"
  final val qfpJoinTripleO = "query.rdf_predicate.join.condition.o"

  final val qfpTotalBits = "universe.nTotalBits"
  final val qfpSpatialBits = "universe.nSpatialBits"
  final val qfpIDsBits = "universe.nIDsBits"
  final val qfpUniverseLatLower = "universe.lat_lower"
  final val qfpUniverseLonLower = "universe.lon_lower"
  final val qfpUniverseLatUpper = "universe.lat_upper"
  final val qfpUniverseLonUpper = "universe.lon_upper"

  final val qfpDicPath = "datasets.dictionary.path"
  final val qfpTriplesPath = "datasets.triples.path"
  final val qfpIntrvlsPath = "datasets.intervals.path"

  final val qfpParseDictionary = "datasets.dictionary.parse"
  final val qfpParseTriples = "datasets.triples.parse"

  final val qfpSparkMaster = "execution.spark_master"

  final val qfpDictionaryTrait = "plans.physical.dictionary"
  final val qfpTriples_trait = "plans.physical.triples"
  final val qfpJoinTriples_trait = "plans.physical.joinTriples"

  final val qfpLogicalPlans = "plans.logical"

  //Query types
  final val starSptRangeQuery = "StarSptRange"
  final val twoHopSptRangeQuery = "JoinSptRange"

  //Logical plans
  final val spatialFirstStarSptRangeLPlan = "SpatialFirstStarSptRange"
  final val spatialFirstJoinStarSptRangeLPlan = "SpatialFirstJoinStStarSptRange"
  final val rdfFirstStarSptRangeLPlan = "RdfFirstStarSptRange"

  final val spatialFirstJoinSptRangeLPlan = "SpatialFirstJoinSptRange"

  //Physical plans
  //TDictionary
  final val tLSDictionary = "LSDictionary"
  final val tSDictionary = "SDictionary"

  //TTriples
  final val tSTriples = "STriples"
  final val tLLLTriples = "LLLTriples"

  //TJoinTriples
  final val tMBJoinSTriples = "MBJoinSTriples"
  final val tMBJoinLLLTriples = "MBJoinLLLTriples"
  
  //field separators
  final val tripleFieldsSeparator = " "
  final val dicFieldsSeparator = "\t"
  final val lonLatSeparator = " "
  
  //RDF URIs
  final val uriMBR = ":hasMBR_WKT"
  final val uriTime = ":SemanticNodeTimeStart"
  
  //Triples fields
  final val tripleSpoStrField = "spoStr"
  final val triplePruneSubKeyField = "pruneSubKey"
  final val tripleSubLongField = "subLong"
  final val triplePredLongField = "predLong"
  final val tripleObjLongField = "objLong"
  final val tripleTranslateSuffix = "_trans"
  final val tripleMBRField = "mbr_wkt"
  final val tripleTimeStartField = "time_start"
  final val tripleJoinKey = "join_key"
  
  //Dictionary fields
  final val dicLineStrField = "lineStr"
  final val dicKeyLongField = "keyLong"
  final val dicValueStrField = "valueStr"

  //parsing
  final val parseString = "S"
  final val parseLongString = "LS"
  final val parseTripleLong = "LLL"
}

