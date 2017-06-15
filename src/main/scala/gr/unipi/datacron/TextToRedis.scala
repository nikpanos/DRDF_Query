package gr.unipi.datacron

import com.typesafe.config.ConfigObject
import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._

import scala.collection.mutable
import scala.io.Source

object TextToRedis {

  /*private def getClusterConnection(configParam: String): RedisCluster = {
    val hosts = AppConfig.getObjectList(configParam)
    val clusterNodes = hosts.toArray.map(x => {
      val y = x.asInstanceOf[ConfigObject]
      val host = y.get(qfpDicRedisAddress).unwrapped().asInstanceOf[String]
      val port = y.get(qfpDicRedisPort).unwrapped().asInstanceOf[Int]
      //println(port)
      ClusterNode(host + ":" + port, host, port)
    })
    new RedisCluster(new mutable.WrappedArray.ofRef(clusterNodes):_*) {
      override val keyTag: Option[KeyTag] = Some(RegexKeyTag)
    }
  }*/

  def main(args : Array[String]): Unit = {
    /*AppConfig.init(args(0))

    val filename = "input/star/dictionarySimple32_8_4.txt"
    val jIdToUri = getClusterConnection(qfpDicRedisIdToUriHosts)
    val jUriToId = getClusterConnection(qfpDicRedisUriToIdHosts)
    jIdToUri.flushall
    jUriToId.flushall

    var count: Int = 0
    for (lin <- Source.fromFile(filename).getLines) {
      val line = lin.trim
      count += 1
      if (count % 10000 == 0) printf("Line: %d\n", count)
      //line = line.trim
      if (line.length == 0) {
        printf("Skipping Line: %d\n", count)
      }
      else {
        val idx: Int = line.indexOf('\t')
        if (idx < 0) {
          throw new RuntimeException(line)
        }
        val id: String = line.substring(0, idx)
        val uri: String = line.substring(idx + 1)

        jIdToUri.set(id, uri)
        jUriToId.set(uri, id)
      }
    }
    println("Success")*/
  }
}
