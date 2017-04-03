package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object DataStore {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private var config: Config = _
  
  def init(_config: Config): Unit = {
    config = _config
  }
  
  lazy val spatialGrid: SpatialGrid = new SpatialGrid(config)
  lazy val temporalGrid: TemporalGrid = new TemporalGrid(config)
  
  lazy val triplesData: DataFrame = new TriplesData(config).data
  lazy val dictionaryData: DataFrame = new DictionaryData(config).data
  
  lazy val spark: SparkSession = SparkSession.builder
      .master(config.getString(Consts.qfpSparkMaster))
      .appName(config.getString(Consts.qfpQueryName))
      .getOrCreate()

  lazy val sc: SparkContext = spark.sparkContext
}