package gr.unipi.datacron.queries

import gr.unipi.datacron.common.{AppConfig, Utils}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.store.DataStore.spark

abstract class BaseQuery() {
  DataStore.init()

  private def executeWarmUp(plan: Option[BaseLogicalPlan]): (Long, Long) = {
    val start = System.currentTimeMillis
    plan.get.preparePlan()
    val startPhysical = System.currentTimeMillis
    val result = plan.get.executePlan
    println(result.count)
    result.show()
    val endTime = System.currentTimeMillis
    val logicalTime = startPhysical - start
    val physicalTime = endTime - startPhysical
    (logicalTime, physicalTime)
  }

  def execute(): Unit = {
    val plan = getExecutionPlan

    if (plan.isDefined) {

      if (AppConfig.stringListContains(qfpQueryOutputDevices, outputDeviceDir) && AppConfig.getBoolean(qfpQueryOutputFolderRemoveExisting)) {
        DataStore.deleteHdfsDirectory(AppConfig.getString(qfpQueryOutputFolderPath))
      }


      //plan.get.preparePlan()


      if (AppConfig.getOptionalBoolean(qfpWarmUpEnabled).getOrElse(false)) {
        println("Warming up...")
        (1 to 10).map(i => (i, executeWarmUp(plan))).foreach(x => println(x._1 + "th warm up time (ms): " + x._2._1 + " logical, " + x._2._2 + " physical."))
      }

      plan.get.doAfterPrepare()

      println("Starting query execution")
      val startTime = System.currentTimeMillis
      plan.get.preparePlan()
      val startPhysicalTime = System.currentTimeMillis
      val result = plan.get.executePlan
      processOutput(result)
      val endTime = System.currentTimeMillis

      val logicalTime = startPhysicalTime - startTime
      val physicalTime = endTime - startPhysicalTime
      val totalTime = endTime - startTime
      if (!AppConfig.getStringList(qfpQueryOutputDevices).contains(outputDeviceWeb)) {
        println("Logical plan build time (ms): " + logicalTime)
        println("Query execution time (ms): " + physicalTime)
        println("Total time (ms): " + totalTime)
      }
    }
    else {
      println("Query execution plan not found")
    }
  }

  protected def processOutput(res: DataFrame): Unit = {
    var result = res
    AppConfig.getStringList(qfpQueryOutputDevices).foreach {
      case `outputDeviceScreen` =>
        if (AppConfig.getBoolean(qfpQueryOutputScreenExplain)) {
          result.explain(true)
        }
        result.show(AppConfig.getInt(qfpQueryOutputScreenHowMany), truncate = false)
        println("Result count: " + result.count)
      case `outputDeviceDir` =>
        /*if (AppConfig.getOptionalBoolean(qfpWebExecution).getOrElse(false)) {
          spark.sql("set spark.sql.parquet.compression.codec=gzip")
          result = result.repartition(1)
        }*/
        AppConfig.getString(qfpQueryOutputFolderFormat) match {
          case `outputFormatParquet` => result.write.parquet(Utils.resolveHdfsPath(qfpQueryOutputFolderPath))
          case `outputFormatText` => result.write.text(Utils.resolveHdfsPath(qfpQueryOutputFolderPath))
          case `outputFormatCSV` => result.write.csv(Utils.resolveHdfsPath(qfpQueryOutputFolderPath))
        }
      case `outputDeviceWeb` =>
        1 to 10 foreach { _ => print("*/") }
        println()
        result.show(10, truncate = false)
        println("Total result count: " + result.count)
        1 to 10 foreach { _ => print("/*") }
        println()
    }
  }

  protected[queries] def getExecutionPlan: Option[BaseLogicalPlan]

}
