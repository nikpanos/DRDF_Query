package gr.unipi.datacron.queries

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.store.HdfsData

abstract class BaseQuery(config: Config) {
  val queryName: String = config.getString(Consts.qfpQueryName)
  val nTotalBits: Int = config.getInt(Consts.qfpTotalBits)
  val nSpatialBits: Int = config.getInt(Consts.qfpSpatialBits)
  val nIDsBits: Int = config.getInt(Consts.qfpIDsBits)
  val data: HdfsData = new HdfsData(config)
  
  def executeQuery(): Boolean
}