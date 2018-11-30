package gr.unipi.datacron

import org.apache.spark.unsafe.hash.Murmur3_x86_32

object NewTest {

  def getPartition(key: Int, n: Int): Int = {
    val k = Murmur3_x86_32.hashInt(key, 42)
    val r = k % n
    if (r < 0) {
      (r + n) % n
    } else r
  }

  def main(args: Array[String]): Unit = {
    Array.range(0, 18).map(x => getPartition(x, 18)).foreach(println)
  }

}
