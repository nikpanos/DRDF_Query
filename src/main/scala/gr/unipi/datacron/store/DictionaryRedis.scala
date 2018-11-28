package gr.unipi.datacron.store

import com.typesafe.config.ConfigObject
import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.store.utils.MyRedisCluster
import redis.clients.jedis._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

class DictionaryRedis() {
  private def getClusterConnection(configParam: String, dbIndex: Int): MyRedisCluster = {
    val hosts = AppConfig.getObjectList(configParam)
    //val clusterNodes = new util.HashSet[HostAndPort]
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

  def getLowerTimestampIdx(timestamp: Long): Int = getUpperTimestampIdx(timestamp)

  def getUpperTimestampIdx(timestamp: Long): Int = Try(dynamicIdToUri.findInSetLowerNow(redisKeyTimestamps, timestamp).toInt).getOrElse(0)

  def getEncodedValue(key: String): Option[Long] = Try(staticUriToId.getNow(key).toLong).toOption


  //-------------------------------------- BATCH REDIS PROCESSING BELOW THIS LINE --------------------------------------
  val hashMap = new mutable.HashMap[Long, Response[String]]()

  def getDecodedValueLater(key: Long): Response[String] = {
    val ret = hashMap.get(key)
    if (ret.isDefined) {
      ret.get
    }
    else {
      val ret1 = getDecodedValueLaterInternal(key)
      hashMap.put(key, ret1)
      ret1
    }
  }

  private def getDecodedValueLaterInternal(key: Long): Response[String] = {
    if (key < 0) {
      staticIdToUri.getLater(key.toString)
    }
    else {
      dynamicIdToUri.getLater(key.toString)
    }
  }

  def syncAllBatch(): Unit = {
    staticIdToUri.syncPipes()
    dynamicIdToUri.syncPipes()
  }

  def getDecodedValueResponse(key: Long): String = {
    hashMap(key).get()
  }

  def close(): Unit = {
    staticIdToUri.close()
    staticUriToId.close()

    dynamicIdToUri.close()
    dynamicUriToId.close()

    hashMap.clear()
  }
}
