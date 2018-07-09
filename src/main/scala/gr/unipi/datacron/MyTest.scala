package gr.unipi.datacron

import java.text.SimpleDateFormat

object MyTest {

  def main(args : Array[String]): Unit = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")

    val d = dateFormat.parse("2016-04-17T12:00:00")
    println(d.getTime)
  }
}
