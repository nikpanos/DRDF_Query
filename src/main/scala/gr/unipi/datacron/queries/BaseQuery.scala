package gr.unipi.datacron.queries

import gr.unipi.datacron.common.{AppConfig, Utils}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.BaseLogicalPlan

abstract class BaseQuery() {
  DataStore.init()

  def execute(): Unit = {
    val plan = getExecutionPlan()

    if (plan.isDefined) {

      if (AppConfig.stringListContains(qfpQueryOutputDevices, outputDeviceDir) && AppConfig.getBoolean(qfpQueryOutputFolderRemoveExisting)) {
        DataStore.deleteHdfsDirectory(AppConfig.getString(qfpQueryOutputFolderPath))
      }

      println("Starting query execution")
      val startTime = System.currentTimeMillis
      val result = plan.get.executePlan
      processOutput(result)
      val endTime = System.currentTimeMillis
      println("Query execution completed")
      println("Query execution time (ms): " + (endTime - startTime))
    }
    else {
      println("Query execution plan not found")
    }
  }

  protected def processOutput(result: DataFrame): Unit = {
    AppConfig.getStringList(qfpQueryOutputDevices).foreach {
      case `outputDeviceScreen` => {
        if (AppConfig.getBoolean(qfpQueryOutputScreenExplain)) {
          result.explain(true)
        }
        result.show(AppConfig.getInt(qfpQueryOutputScreenHowMany), false)
      }
      case `outputDeviceDir` => {
        AppConfig.getString(qfpQueryOutputFolderFormat) match {
          case `outputFormatParquet` => result.write.parquet(Utils.resolveHdfsPath(AppConfig.getString(qfpQueryOutputFolderPath)))
          case `outputFormatText` => result.write.text(Utils.resolveHdfsPath(AppConfig.getString(qfpQueryOutputFolderPath)))
          case `outputFormatCSV` => result.write.csv(Utils.resolveHdfsPath(AppConfig.getString(qfpQueryOutputFolderPath)))
        }
      }
    }
  }

  protected[queries] def getExecutionPlan(): Option[BaseLogicalPlan]

}
