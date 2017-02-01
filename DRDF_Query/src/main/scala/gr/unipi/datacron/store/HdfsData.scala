package gr.unipi.datacron.store

import com.typesafe.config.Config
import gr.unipi.datacron.common._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class HdfsData(config: Config) {
  val conf = new SparkConf().setAppName(config.getString(Consts.qfpQueryName)).setMaster(Consts.qfpSparkMaster)
  val sc = new SparkContext(conf)
  
  val dictionary = sc.textFile(Consts.qfpDicPath)
  val intervals = sc.textFile(Consts.qfpIntrvlsPath)
  val triples = sc.textFile(Consts.qfpTriplesPath)
}