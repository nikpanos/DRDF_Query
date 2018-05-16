package gr.unipi.datacron

import com.redis.cluster._
import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.functions._

import collection.JavaConverters._

object MyTest {
  def main(args : Array[String]) {
    val c: RedisCluster = new RedisCluster() {
      override val keyTag: Option[KeyTag] = None
    }
    //c.no
  }
}
