package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.store.{DataStore, DictionaryRedis}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StringType
import redis.clients.jedis.Response

case class RdsBatchDictionary() extends BaseRdsDictionary {
  protected override def decodeColumn(df: DataFrame, columnName: String, newColumnName: String): DataFrame = {

    /*val bCnf = DataStore.bConfig

    val structType = df.schema.add(newColumnName, StringType, nullable = true)

    implicit val encoder: ExpressionEncoder[Row] = RowEncoder.apply(structType)

    val func: Iterator[Row] => Iterator[Row] = (iter: Iterator[Row]) => {
      if (!AppConfig.isAssigned) {
        AppConfig.setConfig(bCnf.value)
      }
      val iterBatch: Iterator[Row] = iter.map(row => {
        val decoded = DataStore.dictionaryRedis.getDecodedValue(row.getAs[Long](columnName))
        val r: Row = new GenericRowWithSchema((row.toSeq ++ decoded).toArray, structType)
        r
      })
      iterBatch
    }

    df.mapPartitions(func)*/

    val bCnf = DataStore.bConfig

    val structType = df.schema.add(newColumnName, StringType, nullable = true)

    val encodedFieldIndex = df.schema.fieldIndex(columnName)

    implicit val encoder: ExpressionEncoder[Row] = RowEncoder.apply(structType)

    /*val func: Iterator[Row] => Iterator[Row] = (iter: Iterator[Row]) => {
      if (!AppConfig.isAssigned) {
        AppConfig.setConfig(bCnf.value)
      }
      val arr = iter.toArray
      val dicRedis = new DictionaryRedis()
      arr.foreach(row => dicRedis.createResponseValueBatch(row.getLong(encodedFieldIndex)) )

      dicRedis.syncAllBatch()

      val res = arr.map(row => {
        val decoded = DataStore.dictionaryRedis.getDecodedValueBatch(row.getLong(encodedFieldIndex))
        val r: Row = new GenericRowWithSchema((row.toSeq ++ decoded).toArray, structType)
        r
      }).toIterator

      dicRedis.clearResponses()
      res
    }

    df.mapPartitions(func)*/
    df
  }
}
