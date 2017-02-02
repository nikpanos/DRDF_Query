package gr.unipi.datacron.common

import com.typesafe.config.Config

class QueryInfo(config: Config) extends Serializable {
  val qLatLower: Double = config.getDouble(Consts.qfpLatLower)
  val qLonLower: Double = config.getDouble(Consts.qfpLonLower)

  val qLatUpper: Double = config.getDouble(Consts.qfpLatUpper)
  val qLonUpper: Double = config.getDouble(Consts.qfpLonUpper)

  val qTimeLower: Long = config.getLong(Consts.qfpTimeLower)
  val qTimeUpper: Long = config.getLong(Consts.qfpTimeUpper)
}