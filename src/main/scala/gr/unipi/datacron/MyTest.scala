package gr.unipi.datacron

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.unsafe.hash.Murmur3_x86_32

import scala.collection.mutable

object MyTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark: SparkSession = SparkSession.builder
      .master("local[1]")
      .appName("test")
      .getOrCreate()
    import spark.implicits._
    val df = spark.read.csv("C:\\Users\\Panagiotis\\Desktop\\Spatio-textual-Query-master\\Dataset\\partitiondata.csv").toDF("Id", "Text", "Latitude", "Longitude")
    //convert latitude and longitude to float type

    val dataset = df.withColumn("Latitude", regexp_replace(df("Latitude"), "[\"(]", "").cast(DoubleType))
      .withColumn("Longitude", regexp_replace(df("Longitude"), "[)\"]", "").cast(DoubleType))
      .withColumn("Id", trim(df("Id")).cast(IntegerType))

    val aggs = dataset.agg(max(dataset("Latitude")), min(dataset("Latitude")), max(dataset("Longitude")), min(dataset("Longitude"))).head()
    val maxLat = aggs(0).asInstanceOf[Double]
    val minLat = aggs(1).asInstanceOf[Double]
    val maxLon = aggs(2).asInstanceOf[Double]
    val minLon = aggs(3).asInstanceOf[Double]

    //the id of each cell of the grid
    //val ids = np.zeros(dataset.count())

    val stepLon = (maxLon - minLon) / 10
    val stepLat = (maxLat - minLat) / 10

    val df2 = dataset.withColumn("gridID", ((dataset("Longitude") - minLon) / stepLon).cast(IntegerType) * 10 + ((dataset("Latitude") - minLat) / stepLat).cast(IntegerType))

    //retain the distinct values of the cells which correspond the points
    val mapping = df2.select(df2("gridID")).distinct().rdd.map(x => x(0)).collect().zipWithIndex.map(x => (x._1.asInstanceOf[Int], x._2)).toMap
    val mapToId = udf((gridId: Int) => mapping(gridId))
    val df3 = df2.withColumn("partitionID", mapToId(df2("gridID")))

    //partition by the distinct cell id of the grid
    /*val result = df3.select(df3("partitionID"), df3("Id")).rdd.map(x => (x.getAs[Int](0), x.getAs[Int](1))).partitionBy(new Partitioner {
      override def numPartitions: Int = mapping.size

      override def getPartition(key: Any): Int = {
        val k = Murmur3_x86_32.hashInt(key.asInstanceOf[Int], 42)
        val r = k % numPartitions
        if (r < 0) {
          (r + numPartitions) % numPartitions
        } else r
      }
      //override def getPartition(key: Any): Int = mapping(key.asInstanceOf[Int])
    }).toDF("partitionID", "Id")*/
    //result.show
    //val result = df2.select("gridID", df2.columns:_*).rdd.partitionBy(mapping.size, lambda k: mapping(k).values().toDF(df2.schema))

    def printResults(df: DataFrame): Unit = {
      val rdd = df.rdd
      println("Number of partitions: %d".format(rdd.getNumPartitions))
      val counts = df.rdd.mapPartitions(rows => {
        //Iterator(rows.count(_ => true))
        Iterator(rows.toArray.length)//map(x => x.getAs[Int]("partitionID")).toSet.toIterator
      }).collect()

      println("Total sum of all counts: %d\n".format(counts.sum))

      counts.foreach(println)
    }

    //printResults(result)
    println("\n\n")

    val newDf = df3.repartition(mapping.size, col("partitionID"))
    //newDf.explain()
    //printResults(newDf)

    printResults(newDf)
  }
}
