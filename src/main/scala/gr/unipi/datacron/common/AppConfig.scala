package gr.unipi.datacron.common

import java.io.File
import java.util

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import gr.unipi.datacron.common.Consts.qfpSparkMaster

object AppConfig extends Serializable {
  private var queryFile: File = _
  private var config: Config = _

  def init(filename: String): Unit = {
    queryFile = new File(filename)
    config = ConfigFactory.parseFile(queryFile)
  }

  def getString(s: String): String = config.getString(s)

  def getInt(s: String): Int = config.getInt(s)

  def getDouble(s: String): Double = config.getDouble(s)

  def getLong(s: String): Long = config.getLong(s)

  def getStringList(s: String): java.util.List[String] = config.getStringList(s)

  def getObjectList(s: String): util.List[_ <: ConfigObject] = config.getObjectList(s)

  def getBoolean(s: String): Boolean = config.getBoolean(s)

  def yarnMode: Boolean = getString(qfpSparkMaster).equals("yarn")

  def getConfig: String = config.root().render(ConfigRenderOptions.concise())

  def setConfig(newConfig: String): Unit = config = ConfigFactory.parseString(newConfig)

  def isAssigned: Boolean = config != null
}
