package gr.unipi.datacron

import gr.unipi.datacron.common.Consts.lonLatSeparator

object MyTest {

  def main(args : Array[String]): Unit = {
    val decodedSpatial = "POINT (50.1546 48.234)"
    val pos = decodedSpatial.lastIndexOf(lonLatSeparator)
    val lon = decodedSpatial.substring(7, pos)
    val lat = decodedSpatial.substring(pos + 1, decodedSpatial.length - 1)

    println("lon = " + lon + "~")
    println("lat = " + lat + "~")
  }
}
