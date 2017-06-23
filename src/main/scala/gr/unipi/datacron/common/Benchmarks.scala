package gr.unipi.datacron.common

import gr.unipi.datacron.common.Consts.qfpBenchmarkEnabled
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ListBuffer

object Benchmarks {
  private val benchmarkList: ListBuffer[BenchmarkInfo] = new ListBuffer[BenchmarkInfo]()

  def addBenchmark(info: BenchmarkInfo): Unit = benchmarkList += info

  def doBenchmark[T <: AnyRef](foo: () => T, params: BaseOperatorParams): T = {
    if (params.operationName.isEmpty || !AppConfig.getBoolean(qfpBenchmarkEnabled)) {
      foo()
    }
    else {
      val startTime = System.currentTimeMillis
      var result = foo()

      val resultCount = result match {
        case result: Dataset[Row @unchecked] =>
          //result.cache
          result.count
        case result: Array[String] =>
          result.length.toLong
        case _ => 1L
      }
      val endTime = System.currentTimeMillis
      Benchmarks.addBenchmark(BenchmarkInfo(params.operationName.get, endTime - startTime, resultCount))
      result
    }
  }

  def printAll(): Unit = {
    if (benchmarkList.nonEmpty) {
      println("-------------------------------------------------------------------------------------------------------")
      println("                                        BENCHMARK INFO                                                 ")
      var sumTime = 0L
      println("Action,Time (ms),Tuples")
      benchmarkList.foreach(b => {
        b.printString()
        sumTime += b.time
      })
      println()
      println("Summary," + sumTime)
      println("-------------------------------------------------------------------------------------------------------")
    }
  }
}
