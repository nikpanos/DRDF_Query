package gr.unipi.datacron.common

object Consts {
  //Query file params
  final val qfpQueryName = "query.name"
  final val qfpQueryType = "query.type"

  final val qfpLatLower = "query.predicates.lat_lower"
  final val qfpLonLower = "query.predicates.lon_lower"
  final val qfpAltLower = "query.predicates.alt_lower"
  final val qfpLatUpper = "query.predicates.lat_upper"
  final val qfpLonUpper = "query.predicates.lon_upper"
  final val qfpAltUpper = "query.predicates.alt_upper"
  final val qfpTimeLower = "query.predicates.time_lower"
  final val qfpTimeUpper = "query.predicates.time_upper"

  final val qfpTripleP = "query.predicates.rdf.condition.p"
  final val qfpTripleO = "query.predicates.rdf.condition.o"

  final val qfpJoinKey = "query.rdf_predicate.join.key"
  final val qfpJoinTripleP = "query.rdf_predicate.join.condition.p"
  final val qfpJoinTripleO = "query.rdf_predicate.join.condition.o"

  final val qfpDataPropertyEnabled = "datasets.property.enabled"
  final val qfpDatasetList = "datasets.list"

  final val qfpDicPath = "datasets.dictionary.path"
  final val qfpTriplesPath = "datasets.triples.path"
  final val qfpIntrvlsPath = "datasets.intervals.path"
  final val qfpNodePath = "datasets.node.path"
  final val qfpVesselsPath = "datasets.vessels.path"
  final val qfpNodeLeftoversPath = "datasets.nodeLeftovers.path"

  final val qfpDicType = "datasets.dictionary.type"
  final val qfpDicTypeFile = "file"
  final val qfpDicTypeRedis = "redis"

  final val qfpDicRedisIdToUriHosts = "datasets.dictionary.idToUri.hosts"
  final val qfpDicRedisUriToIdHosts = "datasets.dictionary.uriToId.hosts"

  final val qfpDicRedisDynamicDatabaseID = "datasets.dictionary.dynamicDatabaseID"

  final val qfpStatRedisHost = "datasets.statistics.host"
  final val qfpStatRedisPort = "datasets.statistics.port"

  final val qfpDicRedisAddress = "address"
  final val qfpDicRedisPort = "port"

  final val qfpParseDictionary = "datasets.dictionary.parse"
  final val qfpParseTriples = "datasets.triples.parse"
  final val qfpParseNode = "datasets.node.parse"
  final val qfpParseVessel = "datasets.vessels.parse"
  final val qfpParseNodeLeftovers = "datasets.nodeLeftovers.parse"

  final val qfpSparkMaster = "execution.spark_master"
  final val qfpNamenode = "execution.namenode"
  final val qfpHdfsPrefix = "execution.hdfs_dir_prefix"
  final val qfpVerboseLogging = "execution.verbose_logging"
  //final val qfpBenchmarkEnabled = "execution.benchmark_enabled"
  final val qfpWebExecution = "execution.web"

  final val qfpDictionaryTrait = "query.plans.physical.dictionary"
  final val qfpTriples_trait = "query.plans.physical.triples"
  final val qfpJoinTriples_trait = "query.plans.physical.joinTriples"

  final val qfpLogicalPlans = "query.plans.logical"
  final val qfpLogicalOptimizationFlag = "query.plans.logical.optimizationFlag"

  final val qfpEnableFilterByEncodedInfo = "query.enableFilterByEncodedInfo"
  final val qfpEnableRefinementPushdown = "query.enableRefinementPushdown"
  final val qfpEnableMultipleFilterJoinOr = "query.enableMultipleFilterJoinOr"
  final val qfpEnableResultDecode = "query.enableResultDecode"
  final val qfpWarmUpEnabled = "query.warmUpEnabled"
  final val qfpPrintLogicalTreeEnabled = "query.printLogicalTreeEnabled"
  final val qfpBroadcastThreshold = "query.broadcastThreshold"

  final val qfpQueryOutputDevices = "output.outputDevices"
  final val qfpQueryOutputFolderPath = "output.dirOutput.path"
  final val qfpQueryOutputFolderRemoveExisting = "output.dirOutput.removeExisting"
  final val qfpQueryOutputFolderFormat = "output.dirOutput.format"
  final val qfpQueryOutputScreenHowMany = "output.screenOutput.howMany"
  final val qfpQueryOutputScreenExplain = "output.screenOutput.explain"
  final val qfpQueryOutputShouldBeSorted = "output.shouldBeSorted"

  //Output devices
  final val outputDeviceScreen = "screen"
  final val outputDeviceDir = "dir"
  final val outputDeviceWeb = "web"

  //Output file formats
  final val outputFormatParquet = "parquet"
  final val outputFormatText = "text"
  final val outputFormatCSV = "csv"

  //Query types
  final val starSptRangeQuery = "StarSptRange"
  final val twoHopSptRangeQuery = "JoinSptRange"
  final val chainSptRangeQuery = "ChainSptRange"
  final val sparqlQuery = "Sparql"

  //Logical plans
  final val spatialFirstStarSptRangeLPlan = "SpatialFirstStarSptRange"
  final val spatialFirstJoinStarSptRangeLPlan = "SpatialFirstJoinStStarSptRange"
  final val rdfFirstStarSptRangeLPlan = "RdfFirstStarSptRange"
  final val rdfFirstBestStarSptRangeLPlan = "RdfFirstBestStarSptRange"
  final val propertiesStarSptRangeLPlan = "PropertiesStarSptRange"

  final val spatialFirstJoinSptRangeLPlan = "SpatialFirstJoinSptRange"

  final val propertiesChainQueryPlan = "PropertiesChain"

  //Physical plans
  //TDictionary
  final val tRedisDictionary = "RedisDictionary"
  final val tRedisBatchDictionary = "RedisBatchDictionary"

  //TTriples
  final val tSTriples = "STriples"
  final val tLLLTriples = "LLLTriples"

  //TJoinTriples
  final val tMBJoinSTriples = "MBJoinSTriples"
  final val tMBJoinLLLTriples = "MBJoinLLLTriples"
  final val tAJoinLLLTriples = "AJoinLLLTriples"
  final val tABJoinLLLTriples = "ABJoinLLLTriples"

  //field separators
  final val tripleFieldsSeparator = " "
  final val dicFieldsSeparator = "\t"
  final val lonLatSeparator = ' '

  //RDF URIs
  final val uriMBR = ":hasWKT"
  final val uriTimeStart = ":TimeStart"

  final val uriHasGeometry = ":hasGeometry"
  final val uriHasTemporalFeature = ":hasTemporalFeature"

  //final val uriTemporalShortcut = ":hasShortcutTemporal"
  //final val uriSpatialShortcut = ":hasShortcutSpatial"

  //Triples fields
  final val datacronOntologyNamespace = "http://www.datacron-project.eu/datAcron#"
  final val rdfOntologyNamespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

  final val tripleSpoStrField = "spoStr"
  final val triplePruneSubKeyField = "pruneSubKey"
  final val tripleSubLongField = "subLong"
  final val triplePredLongField = "predLong"
  final val tripleObjLongField = "objLong"
  final val triplePropertiesStrField = "propertiesStr"
  final val tripleTranslateSuffix = "_trans"
  final val tripleMBRField = datacronOntologyNamespace + "shortcutSpatial"
  final val tripleTimeStartField = datacronOntologyNamespace + "shortcutTemporal"
  final val tripleAltitudeField = datacronOntologyNamespace + "hasAltitude"
  final val tripleJoinKey = "join_key"
  final val tripleGeometryField = "geometryWKT"
  final val tripleTemporalField = "temporalFeature"

  final val tripleTemporaryRefinementField = "temporaryRefinement"

  //Dictionary fields
  final val dicLineStrField = "lineStr"
  final val dicKeyLongField = "keyLong"
  final val dicValueStrField = "valueStr"

  //parsing
  final val parseString = "S"
  final val parseLong = "L"
  final val parseParquet = "parquet"

  //spark params
  final val partitionsNumberAfterShuffle = "sparkParams.spark.sql.shuffle.partitions"
  final val autoBroadcastJoinThreshold = "sparkParams.spark.sql.autoBroadcastJoinThreshold"

  final val sparqlQuerySource = "query.sparql.source"

  final val rdfType = rdfOntologyNamespace + "type"
  final val nodeTypes = Array("Node").map(datacronOntologyNamespace + _)
  final val weatherConditionTypes = Array("WeatherCondition").map(datacronOntologyNamespace + _)

  final val datasetAisMedNode = "aisMedNode"
  final val datasetAdsbNode = "adsbNode"
  final val datasetFlightPlansNode = "flightPlansNode"
  final val datasetVessel = "vessel"

  final val redisKeyEncodingLatLower = "settings.min_lat"
  final val redisKeyEncodingLonLower = "settings.min_lon"
  final val redisKeyEncodingLatUpper = "settings.max_lat"
  final val redisKeyEncodingLonUpper = "settings.max_lon"
  final val redisKeyEncodingAltLower = "settings.min_alt"
  final val redisKeyEncodingAltUpper = "settings.max_alt"
  final val redisKeyEncodingBitsTotal = "settings.bits_total"
  final val redisKeyEncodingBitsSpatial = "settings.bits_spatial"
  final val redisKeyEncodingBitsId = "settings.bits_id"
  final val redisKeyTimestamps = "settings.timestamps"
  final val redisKeyDimensions = "settings.dimensions"
  final val redisKeyCurveType = "settings.curveTypeRedisKey"

  final val functionSpatioTemporalBox = datacronOntologyNamespace + "spatioTemporalBox2D"
}

