package gr.unipi.datacron

import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.functions._

object MyTest {

  def main(args: Array[String]): Unit = {
    val spark = DataStore.spark
    val sc = DataStore.sc


    val df = spark.read.parquet("/unipi_datasets/maritime/aisMediterranean/oneTriplesTables/parquet").filter(col("predLong") === -17)
    df.groupBy(col("objLong")).agg(count("*").alias("cnt")).orderBy(desc("cnt")).show

    val colu = "-17"
    val df1 = spark.read.parquet("/unipi_datasets/maritime/aisMediterranean/oneTriplesTables/parquet").na.drop(Array(colu))
    df1.groupBy(col(colu)).agg(count("*").alias("cnt")).orderBy(desc("cnt")).show

    val df2 = spark.read.parquet("/unipi_datasets/maritime/static/oneTriplesTables/parquet")
    df2.filter(col("subLong") === -1906).filter(col("predLong") === -58323307).show
  }
}
