package gr.unipi.datacron.store

import gr.unipi.datacron.common._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object DataStore {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  println("Initializing spark session")
  val spark: SparkSession = SparkSession.builder
    .master(AppConfig.getString(Consts.qfpSparkMaster))
    .appName(AppConfig.getString(Consts.qfpQueryName))
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  def init(): Unit = {
    //do nothing here
    //just a method to initialize the other variables
  }
  
  lazy val spatialGrid: SpatialGrid = new SpatialGrid()
  lazy val temporalGrid: TemporalGrid = new TemporalGrid()
  
  lazy val triplesData: DataFrame = new TriplesData().data
  lazy val dictionaryData: DataFrame = new DictionaryData().data
}