package gr.unipi.datacron.store

import gr.unipi.datacron.common.{AppConfig, BaseOperatorParams, Benchmarks}
import gr.unipi.datacron.common.Consts.{qfpHdfsPrefix, qfpNamenode, qfpTriplesPath}
import org.apache.spark.sql.DataFrame

abstract private[store] class BaseHDFSStore() {
  val dataPath: String = if (AppConfig.yarnMode) {
    AppConfig.getString(qfpNamenode) + AppConfig.getString(qfpHdfsPrefix) + AppConfig.getString(configPropertyForDataPath)
  }
  else {
    AppConfig.getString(configPropertyForDataPath)
  }

  protected def configPropertyForDataPath: String

  protected def datasetName: String

  protected def readDataset(): DataFrame

  private[store] val data: DataFrame = Benchmarks.doBenchmark[DataFrame](readDataset, new BaseOperatorParams() {
    override def operationName: Option[String] = Some("Load " + datasetName + "dataset")
  })

  println(datasetName + " dataset path: " + dataPath)
}
