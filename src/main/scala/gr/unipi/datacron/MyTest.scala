package gr.unipi.datacron

import gr.unipi.datacron.common.TriplesTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

object MyTest {
  def main(args : Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[*]")
      .appName("mytest")
      .getOrCreate()

    /*val sc: SparkContext = spark.sparkContext

    implicit val encoder: ExpressionEncoder[Row] = RowEncoder.apply(structType)

    spark.read.text("test.txt").toDF("str").map(row => {
      val tokenizer = TriplesTokenizer(row.getString(0))

      val subj = tokenizer.getNextToken
      val pred = tokenizer.getNextToken
      val obje = tokenizer.getNextToken

      val schema = StructType(Array(StructField("sub", IntegerType),
                                    StructField(pred.get.toString, IntegerType)))

      new GenericRowWithSchema(Array(subj, obje), schema)
    })*/
  }
}
