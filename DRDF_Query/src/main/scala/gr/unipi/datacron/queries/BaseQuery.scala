package gr.unipi.datacron.queries

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import gr.unipi.datacron.store.ExpData

abstract class BaseQuery(config: Config) {
  val queryName = config.getString(Consts.qfpQueryName)
  val nTotalBits = config.getInt(Consts.qfpTotalBits)
  val nSpatialBits = config.getInt(Consts.qfpSpatialBits)
  val nIDsBits = config.getInt(Consts.qfpIDsBits)
  val data = new ExpData(config)
  
  def executeQuery(): Boolean
}