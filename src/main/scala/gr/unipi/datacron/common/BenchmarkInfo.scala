package gr.unipi.datacron.common

case class BenchmarkInfo(name: String, time: Long, tuples: Long) {
  def printString(): Unit = {
    printf("%s,%d,%d\n", name, time, tuples)
  }
}
