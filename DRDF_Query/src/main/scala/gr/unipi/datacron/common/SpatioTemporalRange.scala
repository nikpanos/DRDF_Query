package gr.unipi.datacron.common

import com.typesafe.config.Config

class SpatioTemporalRange(l: SpatioTemporalInfo, h: SpatioTemporalInfo) extends Serializable {
  val low: SpatioTemporalInfo = l
  val high: SpatioTemporalInfo = h
  
  def this(latLower: Double = 0, lonLower: Double = 0, latUpper: Double = 0, lonUpper: Double = 0, timeLower: Long = 0, timeUpper: Long = 0) =
    this(new SpatioTemporalInfo(latLower, lonLower, timeLower), new SpatioTemporalInfo(latUpper, lonUpper, timeUpper))
}