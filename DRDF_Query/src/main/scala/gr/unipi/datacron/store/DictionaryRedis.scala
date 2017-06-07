package gr.unipi.datacron.store

import gr.unipi.datacron.common.{AppConfig, Consts}
import redis.clients.jedis.Jedis

class DictionaryRedis {
  private val idToUriHosts = AppConfig.getObjectList(Consts.qfpDicRedisIdToUriHosts)
  private val uriToIdHosts = AppConfig.getObjectList(Consts.qfpDicRedisUriToIdHosts)

  private var port = idToUriHosts.get(0).get(Consts.qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  private var addr = idToUriHosts.get(0).get(Consts.qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val idToUri: Jedis = new Jedis(addr, port)

  port = uriToIdHosts.get(0).get(Consts.qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  addr = uriToIdHosts.get(0).get(Consts.qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val uriToId: Jedis = new Jedis(addr, port)

  def getDecodedValue(key: Long): Option[String] = Option(idToUri.get(key.toString))

  def getEncodedValue(key: String): Option[Long] = Option(uriToId.get(key.toString).toLong)
}
