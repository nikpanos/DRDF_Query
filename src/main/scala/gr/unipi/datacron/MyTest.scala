package gr.unipi.datacron

import java.text.SimpleDateFormat
import java.util.Date

object MyTest {
  def main(args : Array[String]): Unit = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startDate = new Date()
    val endDate = new Date()

    startDate.setTime(1451635300000L)
    endDate.setTime(1451636000000L)

    println(format.format(startDate))
    println(format.format(endDate))
  }
}
