package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import org.apache.spark.sql.SparkSession

class HdfsData(config: Config) {
  val sparkSession = SparkSession.builder
    .master(config.getString(Consts.qfpSparkMaster))
    .appName(config.getString(Consts.qfpQueryName))
    .getOrCreate()
      
  val sc = sparkSession.sparkContext
}