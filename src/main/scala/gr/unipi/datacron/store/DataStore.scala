package gr.unipi.datacron.store

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

object DataStore {
  //IMPORTANT: all DataStore fields should be lazy evaluated because DataStore is being initialized on each node
  //Initialization in each node is important for Redis dictionary to work

  lazy val spark: SparkSession = SparkSession.builder
    .master(AppConfig.getString(qfpSparkMaster))
    .appName(AppConfig.getString(qfpQueryName))
    .getOrCreate()

  lazy val sc: SparkContext = spark.sparkContext

  lazy val spatialGrid: SpatialGrid = new SpatialGrid()
  lazy val temporalGrid: TemporalGrid = new TemporalGrid()

  lazy val triples: TriplesData = new TriplesData()
  lazy val triplesData: DataFrame = triples.data

  lazy val node: NodeProperties = new NodeProperties()
  lazy val nodeData: DataFrame = node.data

  lazy val vessels: VesselProperties = new VesselProperties()
  lazy val vesselData: DataFrame = vessels.data

  lazy val dictionaryData: DataFrame = if (AppConfig.getString(qfpDicType).equals(qfpDicTypeFile)) {
    new DictionaryData().data
  }
  else null

  lazy val dictionaryRedis: DictionaryRedis = if (AppConfig.getString(qfpDicType).equals(qfpDicTypeRedis)) {
    new DictionaryRedis()
  }
  else null

  var bConfig: Broadcast[String] = _

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
    spark.sql("set spark.sql.shuffle.partitions=" + AppConfig.getInt(myParam))
    temporalGrid.getIntervalId(0)  //to initialize the temporal grid
  }
}