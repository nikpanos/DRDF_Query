package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ExpData {
  private var _dictionary: DictionaryData = _
  private var _triples: TriplesData = _
  private var _spatialGrid: SpatialGrid = _
  private var _temporalGrid: TemporalGrid = _
  private var _spark: SparkSession = _
  private var _sc: SparkContext = _

  def init(config: Config) = {
    _dictionary = new DictionaryData(config)
    _triples = new TriplesData(config)
    _spatialGrid = new SpatialGrid(config)
    _temporalGrid = new TemporalGrid(config)
    _spark = SparkSession.builder
      .master(config.getString(Consts.qfpSparkMaster))
      .appName(config.getString(Consts.qfpQueryName))
      .getOrCreate()

    _sc = _spark.sparkContext
  }

  lazy val dictionary = _dictionary
  lazy val triples = _triples
  lazy val spatialGrid = _spatialGrid
  lazy val temporalGrid = _temporalGrid
  
  lazy val triplesData = _triples.data
  lazy val dicData = _dictionary.data
  
  lazy val spark: SparkSession = _spark
  lazy val sc = _sc
}