package gr.unipi.datacron.store

import gr.unipi.datacron.common.{BaseOperatorParams, Benchmarks, Utils}
import org.apache.spark.sql.DataFrame

abstract private[store] class BaseHDFSStore() {
  val dataPath: String = Utils.resolveHdfsPath(configPropertyForDataPath)

  protected def configPropertyForDataPath: String

  protected def datasetName: String

  protected def readDataset(): DataFrame

  private[store] val data: DataFrame = Benchmarks.doBenchmark[DataFrame](readDataset, new BaseOperatorParams() {
    override def operationName: Option[String] = Some("Load " + datasetName + "dataset")
  })

  println(datasetName + " dataset path: " + dataPath)
}
