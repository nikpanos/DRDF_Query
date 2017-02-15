package gr.unipi.datacron.common

class SpatioTemporalInfo(lat: Double = 0, lon: Double = 0, t: Long = 0) extends Serializable {
  val latitude = lat
  val longitude = lon
  val time = t
}