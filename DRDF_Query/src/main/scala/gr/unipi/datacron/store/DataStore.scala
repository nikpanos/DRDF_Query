package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataStore {
  private var config: Config = _
  
  def init(_config: Config) = {
    config = _config
  }
  
  lazy val spatialGrid = new SpatialGrid(config)
  lazy val temporalGrid = new TemporalGrid(config)
  
  lazy val triplesData = new TriplesData(config).data
  lazy val dictionaryData = new DictionaryData(config).data
  
  lazy val spark = SparkSession.builder
      .master(config.getString(Consts.qfpSparkMaster))
      .appName(config.getString(Consts.qfpQueryName))
      .getOrCreate()
  lazy val sc = spark.sparkContext
}