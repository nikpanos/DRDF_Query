package gr.unipi.datacron.store

import gr.unipi.datacron.common.{BaseOperatorParams, Benchmarks, Utils}
import gr.unipi.datacron.plans.logical.dynamicPlans.operators.BaseOperator
import org.apache.spark.sql.DataFrame

abstract private[store] class BaseHDFSStore() {
  val dataPath: String = Utils.resolveHdfsPath(configPropertyForDataPath)

  protected def configPropertyForDataPath: String

  protected def datasetName: String

  protected def readDataset(): DataFrame

  private[store] val data: DataFrame = Benchmarks.doBenchmark[DataFrame](readDataset, new BaseOperatorParams() {
    override def logicalOperator: Option[BaseOperator] = None
  })

  println(datasetName + " dataset path: " + dataPath)
}
