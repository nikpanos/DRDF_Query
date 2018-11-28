package gr.unipi.datacron.store

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import redis.clients.jedis.{Jedis, JedisPool}

class StatisticsRedis {

  protected val redisPool: JedisPool = new JedisPool(AppConfig.getString(qfpStatRedisHost), AppConfig.getInt(qfpStatRedisPort))

  def getValue(key: String): Option[String] = {
    var jedis: Jedis = null
    try {
      jedis = redisPool.getResource
      Option(jedis.get(key))
    }
    catch {
      case _: Exception => None
    }
    finally {
      jedis.close()
    }
  }
}
