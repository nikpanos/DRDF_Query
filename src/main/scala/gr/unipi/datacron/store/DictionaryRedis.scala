package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import redis.clients.jedis.Jedis
import gr.unipi.datacron.common.AppConfig

class DictionaryRedis() {
  private val idToUriHosts = AppConfig.getObjectList(qfpDicRedisIdToUriHosts)
  private val uriToIdHosts = AppConfig.getObjectList(qfpDicRedisUriToIdHosts)

  private val port1 = idToUriHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  private val addr1 = uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val idToUri: Jedis = new Jedis(addr1, port1)

  private val port2 = uriToIdHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  private val addr2 = uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val uriToId: Jedis = new Jedis(addr2, port2)

  def getDecodedValue(key: Long): Option[String] = Option(idToUri.get(key.toString))

  def getEncodedValue(key: String): Option[Long] = Option(uriToId.get(key.toString).toLong)
}
