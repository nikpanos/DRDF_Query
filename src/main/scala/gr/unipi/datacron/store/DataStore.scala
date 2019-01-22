package gr.unipi.datacron.store

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataStore {
  //IMPORTANT: all DataStore fields should be lazy evaluated because DataStore is being initialized on each node
  //Initialization in each node is important for Redis dictionary to work

  lazy val spark: SparkSession = SparkSession.builder
    .master(AppConfig.getString(qfpSparkMaster))
    .appName(AppConfig.getString(qfpQueryName))
    .config("spark.ui.showConsoleProgress", !AppConfig.getOptionalBoolean(qfpWebExecution).getOrElse(false))
    .getOrCreate()

  lazy val sc: SparkContext = spark.sparkContext

  lazy val spatialGrid: SpatialGrid = new SpatialGrid()
  lazy val temporalGrid: TemporalGrid = new TemporalGrid()

  lazy val triples: TriplesData = new TriplesData()
  lazy val triplesData: DataFrame = getCached(triples.data)

  lazy val node: NodeProperties = new NodeProperties()
  lazy val nodeData: DataFrame = getCached(node.data)
  var nodeDatasetType: String = "" //only used for TextToParquet

  lazy val vessels: VesselProperties = new VesselProperties()
  lazy val vesselData: DataFrame = getCached(vessels.data)

  lazy val dictionaryRedis: DictionaryRedis = if (AppConfig.getString(qfpDicType).equals(qfpDicTypeRedis)) {
    new DictionaryRedis()
  }
  else null

  lazy val statisticsRedis: StatisticsRedis = new StatisticsRedis()

  var bConfig: Broadcast[String] = _

  private def getCached(df: DataFrame): DataFrame = {
    if (AppConfig.getOptionalBoolean(qfpWarmUpEnabled).getOrElse(false)) df.cache
    else df
  }

  lazy val spatialAndTemporalShortcutCols: Array[String] = if (dictionaryRedis.getDynamicSetting(redisKeyDimensions).get.toInt == 2) {
    Array(tripleMBRField, tripleTimeStartField).map(dictionaryRedis.getEncodedValue(_).get.toString)
  }
  else {
    Array(tripleMBRField, tripleTimeStartField, tripleAltitudeField).map(dictionaryRedis.getEncodedValue(_).get.toString)
  }

  def init(): Unit = {
    //Force initialization of spark context here in order to omit the initialization overhead
    if (!AppConfig.getBoolean(qfpVerboseLogging)) {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
    }
    println("Initializing Spark session and Redis connections")
    bConfig = sc.broadcast(AppConfig.getConfig)
    if (AppConfig.getString(qfpDicType).equals(qfpDicTypeRedis)) {
      dictionaryRedis.getDecodedValue(-1L)
      dictionaryRedis.getEncodedValue("a")
    }
    if (AppConfig.getInt(partitionsNumberAfterShuffle) > 0) {
      spark.sql("set spark.sql.shuffle.partitions=" + AppConfig.getInt(partitionsNumberAfterShuffle))
      val oBroad = AppConfig.getOptionalInt(autoBroadcastJoinThreshold)
      if (oBroad.isDefined) {
        spark.sql("set spark.sql.autoBroadcastJoinThreshold=" + oBroad.get)
      }
    }
    //temporalGrid.getIntervalId(0)  //to initialize the temporal grid

    /*if (AppConfig.getOptionalBoolean(qfpWarmUpEnabled).getOrElse(false)) {
      allData.foreach(df => println(df.count))
    }*/
  }
}