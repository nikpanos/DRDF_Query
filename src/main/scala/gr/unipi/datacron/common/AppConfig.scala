package gr.unipi.datacron.common

import java.io.File
import java.util

import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigRenderOptions}
import gr.unipi.datacron.common.AppConfig.config
import gr.unipi.datacron.common.Consts.qfpSparkMaster

import collection.JavaConverters._
import scala.collection.mutable.Buffer
import scala.util.Try

object AppConfig extends Serializable {
  //private var queryFile: File = _
  private var config: Config = _

  def init(filename: String): Unit = {
    val queryFile: File = new File(filename)
    val fileConfig = ConfigFactory.parseFile(queryFile)
    config = ConfigFactory.load(fileConfig)
  }

  def getString(s: String): String = config.getString(s)

  def getOptionalString(s: String): Option[String] = Try(config.getString(s)).toOption

  def getInt(s: String): Int = config.getInt(s)

  def getOptionalInt(s: String): Option[Int] = Try(config.getInt(s)).toOption

  def getDouble(s: String): Double = config.getDouble(s)

  def getOptionalDouble(s: String): Option[Double] = Try(config.getDouble(s)).toOption

  def getLong(s: String): Long = config.getLong(s)

  def getOptionalLong(s: String): Option[Long] = Try(config.getLong(s)).toOption

  def getStringList(s: String): Array[String] = config.getStringList(s).asScala.toArray

  def stringListContains(config: String, s: String): Boolean = getStringList(config).contains(s)

  def getObjectList(s: String): util.List[_ <: ConfigObject] = config.getObjectList(s)

  def getBoolean(s: String): Boolean = config.getBoolean(s)

  def getOptionalBoolean(s: String): Option[Boolean] = Try(config.getBoolean(s)).toOption

  def yarnMode: Boolean = getString(qfpSparkMaster).equals("yarn")

  def getConfig: String = config.root().render(ConfigRenderOptions.concise())

  def setConfig(newConfig: String): Unit = config = ConfigFactory.parseString(newConfig)

  def isAssigned: Boolean = config != null
}
