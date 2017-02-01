package gr.unipi.datacron.queries

import com.typesafe.config.Config
import gr.unipi.datacron.common._

class StrQuery(config: Config) extends BaseQuery(config) {
  
  val latLower: Double = config.getDouble(Consts.qfpLatLower)
  val lonLower: Double = config.getDouble(Consts.qfpLonLower)
  
  val latUpper: Double = config.getDouble(Consts.qfpLatUpper)
  val lonUpper: Double = config.getDouble(Consts.qfpLonUpper)
  
  val timeLower: Long = config.getLong(Consts.qfpTimeLower)
  val timeUpper: Long = config.getLong(Consts.qfpTimeUpper)
  
  val triple_s: String = config.getString(Consts.qfpTripleS)
  val triple_p: String = config.getString(Consts.qfpTripleP)
  val triple_o: String = config.getString(Consts.qfpTripleO)
  
  override def executeQuery(): Boolean = {
    true
  }
}