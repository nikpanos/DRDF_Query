package gr.unipi.datacron.common

import org.apache.spark.sql.DataFrame

object Benchmarks {

  var isBenchmarkingEnabled = false

  def doBenchmark[T <: DataFrame](foo: () => T, params: BaseOperatorParams): T = {
    if (params.logicalOperator.isEmpty || !isBenchmarkingEnabled) {
      foo()
    }
    else {
      val result = foo()
      val resultCount = result.count()
      params.logicalOperator.get.setRealOutputSize(resultCount)
      result
    }
  }
}
