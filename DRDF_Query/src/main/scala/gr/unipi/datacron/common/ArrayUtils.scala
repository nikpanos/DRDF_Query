package gr.unipi.datacron.common

object ArrayUtils {
  def binarySearch(a: Array[(Long, String)], x: Long): Int = {
    if ((a.length == 0) || (a(0)._1 > x) || (a(a.length - 1)._1 < x)) {
      return -1;
    }
    else {
      var lo = 0;
      var hi = a.length - 1;
      var mid = 0;
      while (lo <= hi) {
        mid = lo + (hi - lo) / 2
        if (x < a(mid)._1) hi = mid - 1
        else if (x > a(mid)._1) lo = mid + 1
        else return mid
      }
      return -1
    }
  }
  
  def binarySearch(a: Array[(String, Long)], x: String): Int = {
    if ((a.length == 0) || (a(0)._1 > x) || (a(a.length - 1)._1 < x)) {
      return -1;
    }
    else {
      var lo = 0;
      var hi = a.length - 1;
      var mid = 0;
      while (lo <= hi) {
        mid = lo + (hi - lo) / 2
        if (x < a(mid)._1) hi = mid - 1
        else if (x > a(mid)._1) lo = mid + 1
        else return mid
      }
      return -1
    }
  }

  //implicit def arrayToKVArray(r: Array[(Int, Int)]) = new KVArrayUtils(r)
}