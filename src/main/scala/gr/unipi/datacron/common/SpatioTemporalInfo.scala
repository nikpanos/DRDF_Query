package gr.unipi.datacron.common

case class SpatioTemporalInfo(latitude: Double, longitude: Double, altitude: Option[Double], time: Long) extends Serializable {
}