package gr.unipi.datacron.store

import java.util
import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.ConfigObject
import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import redis.clients.jedis.{HostAndPort, JedisCluster}
import gr.unipi.datacron.common.RedisUtil
import redis.clients.jedis.Response

import scala.util.Try

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

  protected val idToUri: JedisCluster = getClusterConnection(qfpDicRedisIdToUriHosts)

  protected val uriToId: JedisCluster = getClusterConnection(qfpDicRedisUriToIdHosts)

  def getDecodedValue(key: Long): Option[String] = Try(idToUri.get(key.toString)).toOption

  def getEncodedValue(key: String): Option[Long] = Try(uriToId.get(key).toLong).toOption


  //-------------------------------------- BATCH REDIS PROCESSING BELOW THIS LINE --------------------------------------

  private val nodeMapIdToUri = idToUri.getClusterNodes
  private var anyHost = nodeMapIdToUri.keySet.iterator.next
  private val slotHostMapIdToUri = RedisUtil.getSlotHostMap(anyHost)
  //private val nodeMapUriToId = uriToId.getClusterNodes
  //anyHost = nodeMapUriToId.keySet.iterator.next
  //private val slotHostMapUriToId = RedisUtil.getSlotHostMap(anyHost)
  private val pipeIdToUri = RedisUtil.getPipeDictionary(nodeMapIdToUri)
  //private val pipeUriToId = RedisUtil.getPipeDictionary(nodeMapUriToId)
  private val responses: util.HashMap[Long, Response[String]] = new util.HashMap[Long, Response[String]]()

  def createResponseValueBatch(key: Long): Unit = responses.putIfAbsent(key, getResponseValueBatch(key.toString))

  private def getResponseValueBatch(key: String): Response[String] = pipeIdToUri.get(RedisUtil.getShardName(key, slotHostMapIdToUri)).get(key)

  def getDecodedValueBatch(key: Long): Option[String] = {
    if (responses.containsKey(key)){
      Some(responses.get(key).get())
    }
    else {
      throw new Exception("AAAAAAAAAAAAAAAAAAA")
    }
  }

  def syncAllBatch(): Unit = RedisUtil.syncAll(pipeIdToUri.values())

  def clearResponses(): Unit = responses.clear()
}
