package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import redis.clients.jedis.Jedis
import com.typesafe.config.Config
import gr.unipi.datacron.common.AppConfig

class DictionaryRedis() {
  private[store] val idToUriHosts = AppConfig.getObjectList(qfpDicRedisIdToUriHosts)
  private[store] val uriToIdHosts = AppConfig.getObjectList(qfpDicRedisUriToIdHosts)

  private var port = idToUriHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  private var addr = uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val idToUri: Jedis = new Jedis(addr, port)

  port = uriToIdHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  addr = uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val uriToId: Jedis = new Jedis(addr, port)

  def getDecodedValue(key: Long): Option[String] = Option(idToUri.get(key.toString))

  def getEncodedValue(key: String): Option[Long] = Option(uriToId.get(key.toString).toLong)
}
