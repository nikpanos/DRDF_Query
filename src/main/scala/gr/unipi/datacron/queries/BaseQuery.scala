package gr.unipi.datacron.queries

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.BaseLogicalPlan

abstract class BaseQuery() {
  DataStore.init()

  def execute(): Unit = {
    val plan = getExecutionPlan()

    if (plan.isDefined) {
      println("Starting query execution")
      val startTime = System.currentTimeMillis
      val result = plan.get.executePlan
      val endTime = System.currentTimeMillis
      processOutput(result)
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
        if (AppConfig.getBoolean(qfpQueryOutputFolderRemoveExisting)) {
          DataStore.deleteHdfsDirectory(AppConfig.getString(qfpQueryOutputFolderPath))
        }

        AppConfig.getString(qfpQueryOutputFolderFormat) match {
          case `outputFormatParquet` => result.write.parquet(AppConfig.getString(qfpQueryOutputFolderPath))
          case `outputFormatText` => result.write.text(AppConfig.getString(qfpQueryOutputFolderPath))
          case `outputFormatCSV` => result.write.csv(AppConfig.getString(qfpQueryOutputFolderPath))
        }
      }
    }
  }

  protected[queries] def getExecutionPlan(): Option[BaseLogicalPlan]

}
