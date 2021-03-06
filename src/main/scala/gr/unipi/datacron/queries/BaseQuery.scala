package gr.unipi.datacron.queries

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.{AppConfig, Benchmarks, Utils}
import gr.unipi.datacron.plans.logical.BaseLogicalPlan
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.{DataFrame, SaveMode}

abstract class BaseQuery() {
  DataStore.init()

  private def executeWarmUp(plan: Option[BaseLogicalPlan]): (Long, Long) = {
    val logicalStart = System.currentTimeMillis
    plan.get.preparePlan()
    val logicalEnd = System.currentTimeMillis

    val result = plan.get.executePlan
    val startPhysical = System.currentTimeMillis
    val count = result.count
    val endPhysical = System.currentTimeMillis
    println(count)
    //result.show()
    val logicalTime = logicalEnd - logicalStart
    val physicalTime = endPhysical - startPhysical
    (logicalTime, physicalTime)
  }

  def execute(): Unit = {
    val plan = getExecutionPlan

    if (plan.isDefined) {

      /*if (AppConfig.stringListContains(qfpQueryOutputDevices, outputDeviceDir) && AppConfig.getBoolean(qfpQueryOutputFolderRemoveExisting)) {
        DataStore.deleteHdfsDirectory(AppConfig.getString(qfpQueryOutputFolderPath))
      }*/


      //plan.get.preparePlan()


      if (AppConfig.getOptionalBoolean(qfpWarmUpEnabled).getOrElse(false)) {
        println("Warming up...")
        (1 to 10).map(i => (i, executeWarmUp(plan))).foreach(x => println(x._1 + "th warm up time (ms): " + x._2._1 + " logical, " + x._2._2 + " physical."))
        //plan.get.doAfterPrepare()
      }

      println("Starting query execution")
      val startLogicalTime = System.currentTimeMillis
      plan.get.preparePlan()
      if (AppConfig.getOptionalBoolean(qfpPrintLogicalTreeEnabled).getOrElse(false)) {
        plan.get.doAfterPrepare()
      }
      val endLogicalTime = System.currentTimeMillis
      val result = plan.get.executePlan
      processOutput(result)

      val logicalTime = endLogicalTime - startLogicalTime
      //val totalTime = physicalTime + logicalTime
      if (!AppConfig.getStringList(qfpQueryOutputDevices).contains(outputDeviceWeb)) {
        println("Logical plan build time (ms): " + logicalTime)
        //println("Query execution time (ms): " + physicalTime)
        //println("Total time (ms): " + totalTime)
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
        val startTime = System.currentTimeMillis
        val count = result.count()
        val endTime = System.currentTimeMillis
        result.show(AppConfig.getInt(qfpQueryOutputScreenHowMany), truncate = false)
        if (AppConfig.getBoolean(qfpQueryOutputScreenExplain)) {
          result.explain(true)
        }
        println("Result count: " + count)
        println("Query execution time (ms): " + (endTime - startTime))
      case `outputDeviceDir` =>
        /*if (AppConfig.getOptionalBoolean(qfpWebExecution).getOrElse(false)) {
          spark.sql("set spark.sql.parquet.compression.codec=gzip")
          result = result.repartition(1)
        }*/
        val mode = if (AppConfig.stringListContains(qfpQueryOutputDevices, outputDeviceDir) && AppConfig.getBoolean(qfpQueryOutputFolderRemoveExisting)) {
          SaveMode.Overwrite
        }
        else {
          SaveMode.ErrorIfExists
        }
        AppConfig.getString(qfpQueryOutputFolderFormat) match {
          case `outputFormatParquet` => result.write.mode(mode).parquet(Utils.resolveHdfsPath(qfpQueryOutputFolderPath))
          case `outputFormatText` => result.write.mode(mode).text(Utils.resolveHdfsPath(qfpQueryOutputFolderPath))
          case `outputFormatCSV` => result.write.mode(mode).csv(Utils.resolveHdfsPath(qfpQueryOutputFolderPath))
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
