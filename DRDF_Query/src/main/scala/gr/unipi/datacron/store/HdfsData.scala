package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class HdfsData(config: Config) {
  val conf = new SparkConf().setAppName(config.getString(Consts.qfpQueryName))
  conf.setMaster(config.getString(Consts.qfpSparkMaster)) //This may be removed later
  val sc = new SparkContext(conf)
  
}