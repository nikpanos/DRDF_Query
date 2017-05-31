package gr.unipi.datacron.common

import java.io.File

import com.typesafe.config.ConfigFactory

object AppConfig {
  private var queryFile: File = _
  private lazy val config = ConfigFactory.parseFile(queryFile)

  def init(filename: String): Unit = {
    queryFile = new File(filename)
  }

  def getString(s: String): String = config.getString(s)

  def getInt(s: String): Int = config.getInt(s)

  def getDouble(s: String): Double = config.getDouble(s)

  def getLong(s: String): Long = config.getLong(s)

  def getStringList(s: String): java.util.List[String] = config.getStringList(s)
}
