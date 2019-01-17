package gr.unipi.datacron

import java.text.SimpleDateFormat
import java.util.Date

object MyTest {

  def main(args : Array[String]): Unit = {
    val dt = new Date()
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dt.setTime(1460926796000L)
    println(df.format(dt))
  }
}
