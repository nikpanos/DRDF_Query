package gr.unipi.datacron.store

import java.util
import java.util.HashSet

import com.typesafe.config.ConfigObject
import gr.unipi.datacron.common.AppConfig
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}
import gr.unipi.datacron.common.Consts._
//import gr.unipi.datacron.common.AppConfig

class DictionaryRedis() {
  private val idToUriHosts  = AppConfig.getObjectList(qfpDicRedisIdToUriHosts)
  private val uriToIdHosts = AppConfig.getObjectList(qfpDicRedisUriToIdHosts)

  val jedisClusterNodes1 = getNodes(idToUriHosts)
  val jedisClusterNodes2 = getNodes(uriToIdHosts)

  private def getNodes(list: util.List[_ <: ConfigObject]): HashSet[HostAndPort] = {
    val result = new HashSet[HostAndPort]();
    val tmp = list.toArray
    list.toArray.foreach(x => {
      val y = x.asInstanceOf[ConfigObject]
      result.add(new HostAndPort(y.get(qfpDicRedisAddress).unwrapped().asInstanceOf[String],
                                 y.get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]))
    })
    result
  }

  //private val port1 = idToUriHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  //private val addr1 = uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val idToUri = new JedisCluster(jedisClusterNodes1)

  //private val port2 = uriToIdHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  //private val addr2 = uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val uriToId = new JedisCluster(jedisClusterNodes2)

  def getDecodedValue(key: Long): Option[String] = Option(idToUri.get(key.toString))

  def getEncodedValue(key: String): Option[Long] = Option(uriToId.get(key.toString).toLong)
}
