package gr.unipi.datacron.store

import java.util

import com.typesafe.config.ConfigObject
import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.mutable

class DictionaryRedis() {
  private def getClusterConnection(configParam: String): JedisCluster = {
    val hosts = AppConfig.getObjectList(configParam)
    val clusterNodes = new util.HashSet[HostAndPort]
    hosts.toArray.foreach(x => {
      val y = x.asInstanceOf[ConfigObject]
      val host = y.get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
      val port = y.get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
      clusterNodes.add(new HostAndPort(host, port))
    })
    new JedisCluster(clusterNodes)
  }

  /*private def getClusterConnection(configParam: String): RedisCluster = {
    val hosts = AppConfig.getObjectList(configParam)
    val clusterNodes = hosts.toArray.map(x => {
      val y = x.asInstanceOf[ConfigObject]
      val host = y.get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
      val port = y.get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
      ClusterNode(host + ":" + port, host, port)
    })
    new RedisCluster(new mutable.WrappedArray.ofRef(clusterNodes):_*) {
      override val keyTag: Option[KeyTag] = None
    }
  }*/

  private val idToUri = getClusterConnection(qfpDicRedisIdToUriHosts)

  private val uriToId = getClusterConnection(qfpDicRedisUriToIdHosts)

  def getDecodedValue(key: Long): Option[String] = Some(idToUri.get(key.toString))

  def getEncodedValue(key: String): Option[Long] = Some(uriToId.get(key.toString).toLong)
}
