package gr.unipi.datacron.common

object ArrayUtils {
  implicit class KVArrayUtils(a: Array[(Long, Long)]) {
    def binarySearch(x: Long): Int = {
      var lo = 0;
      var hi = a.length - 1;
      var mid = 0;
      while (lo <= hi) {
        mid = lo + (hi - lo) / 2
        if      (x < a(mid)._1) hi = mid - 1
        else if (x > a(mid)._1) lo = mid + 1
        else return mid
      }
      return -1
    }
  }
  
  //implicit def arrayToKVArray(r: Array[(Int, Int)]) = new KVArrayUtils(r)
}