package gr.unipi.datacron.store

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.ConfigObject
import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import redis.clients.jedis.{HostAndPort, JedisCluster}
import gr.unipi.datacron.store.utils.MyRedisCluster
import redis.clients.jedis.Response

import scala.util.Try

import collection.JavaConverters._

class DictionaryRedis() {
  private def getClusterConnection(configParam: String, dbIndex: Int): MyRedisCluster = {
    val hosts = AppConfig.getObjectList(configParam)
    val clusterNodes = new util.HashSet[HostAndPort]
    val hostsAndPorts = hosts.toArray.map(x => {
      val y = x.asInstanceOf[ConfigObject]
      val host = y.get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
      val port = y.get(qfpDicRedisPort).unwrapped().asInstanceOf[Int] + (2 * dbIndex)
      new HostAndPort(host, port)
    }).toSet.asJava
    new MyRedisCluster(hostsAndPorts)
  }

  protected val staticIdToUri: MyRedisCluster = getClusterConnection(qfpDicRedisIdToUriHosts, 0)
  protected val staticUriToId: MyRedisCluster = getClusterConnection(qfpDicRedisUriToIdHosts, 0)

  protected val dynamicIdToUri: MyRedisCluster = getClusterConnection(qfpDicRedisIdToUriHosts, AppConfig.getInt(qfpDicRedisDynamicDatabaseID) + 1)
  protected val dynamicUriToId: MyRedisCluster = getClusterConnection(qfpDicRedisUriToIdHosts, AppConfig.getInt(qfpDicRedisDynamicDatabaseID) + 1)

  def getDecodedValue(key: Long): Option[String] = if (key < 0) {
    Try(staticIdToUri.getNow(key.toString)).toOption
  }
  else {
    Try(dynamicIdToUri.getNow(key.toString)).toOption
  }

  def getDynamicSetting(key: String): Option[String] = Try(dynamicIdToUri.getNow(key)).toOption

  def getEncodedValue(key: String): Option[Long] = Try(staticUriToId.getNow(key).toLong).toOption


  //-------------------------------------- BATCH REDIS PROCESSING BELOW THIS LINE --------------------------------------

}
