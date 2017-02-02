package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._

class ExpData(config: Config) {
  val hdfs = new HdfsData(config)
  val dictionary = new DictionaryData(config, this)
  val triples = new TriplesData(config, this)
  val spatialGrid = new SpatialGrid(config, this)
  val temporalGrid = new TemporalGrid(config, this)
  
  val triplesData = triples.data
  val dicData = dictionary.data
}