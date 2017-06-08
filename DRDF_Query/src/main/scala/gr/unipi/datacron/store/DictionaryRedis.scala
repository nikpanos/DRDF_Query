package gr.unipi.datacron.store

import com.typesafe.config.ConfigObject
import gr.unipi.datacron.common.Consts._
import redis.clients.jedis.Jedis

class DictionaryRedis() {
  //private[store] val  = AppConfig.getObjectList(Consts.qfpDicRedisIdToUriHosts)
  //private[store] val  = AppConfig.getObjectList(Consts.qfpDicRedisUriToIdHosts)

  private var port = 6379  //idToUriHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  private var addr = "localhost"  //uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val idToUri: Jedis = new Jedis(addr, port)

  port = 6380 //uriToIdHosts.get(0).get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
  addr = "localhost" //uriToIdHosts.get(0).get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
  private val uriToId: Jedis = new Jedis(addr, port)

  def getDecodedValue(key: Long): Option[String] = Option(idToUri.get(key.toString))

  def getEncodedValue(key: String): Option[Long] = Option(uriToId.get(key.toString).toLong)
}
