package gr.unipi.datacron.plans.physical.dictionary

import gr.unipi.datacron.common.AppConfig
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.traits.decodeColumnsParams
import gr.unipi.datacron.store.{DataStore, DictionaryRedis}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

case class RdsBatchDictionary() extends BaseRdsDictionary {
  protected override def decodeColumn(df: DataFrame, columnName: String, newColumnName: String): DataFrame = {
    val bCnf = DataStore.bConfig

    val encodedFieldIndex = df.schema.fieldIndex(columnName)
    val shouldReplace = df.columns.contains(newColumnName)
    val structType: StructType = if (shouldReplace) StructType(df.schema.updated(encodedFieldIndex, StructField(newColumnName, StringType, nullable = true)))
    else df.schema.add(newColumnName, StringType, nullable = true)

    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(structType)

    val func: Iterator[Row] => Iterator[Row] = (iter: Iterator[Row]) => {
      if (!AppConfig.isAssigned) {
        AppConfig.setConfig(bCnf.value)
      }
      val dicRedis = new DictionaryRedis
      val arr = iter.map(row => (row, dicRedis.getDecodedValueLater(row.getLong(encodedFieldIndex)))).toArray

      dicRedis.syncAllBatch()

      arr.map(d => {
        val row = d._1
        val decoded = d._2.get()

        val data: Array[Any] = if (shouldReplace) row.toSeq.updated(encodedFieldIndex, decoded).toArray
        else (row.toSeq :+ decoded).toArray
        val r: Row = new GenericRowWithSchema(data, structType)
        r
      }).toIterator
    }
    df.mapPartitions(func)
  }

  override def decodeColumns(params: decodeColumnsParams): DataFrame = {
    val df = params.df
    val preserveColumnNames = params.preserveColumnNames
    val cols = params.columnNames.map(c => {
      val encodedFieldIndex = df.schema.fieldIndex(c)
      if (preserveColumnNames) {
        (c, encodedFieldIndex)
      }
      else {
        (c + tripleTranslateSuffix, encodedFieldIndex)
      }
    })

    val bCnf = DataStore.bConfig
    val structType: StructType = if (preserveColumnNames) {
      cols.foldLeft(df.schema)((s, c) => {
        StructType(s.updated(c._2, StructField(c._1, StringType, nullable = true)))
      })
    }
    else {
      cols.foldLeft(df.schema)((s, c) => {
        s.add(c._1, StringType, nullable = true)
      })

    }

    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(structType)

    val func: Iterator[Row] => Iterator[Row] = (iter: Iterator[Row]) => {
      if (!AppConfig.isAssigned) {
        AppConfig.setConfig(bCnf.value)
      }
      val dicRedis = new DictionaryRedis
      val arr = iter.map(row => (row, cols.map(c => (c._1, c._2, dicRedis.getDecodedValueLater(row.getLong(c._2)))))).toArray

      dicRedis.syncAllBatch()

      val res = arr.map(d => {
        val row = d._1
        val cols = d._2

        val data: Array[Any] = if (preserveColumnNames) {
          cols.foldLeft(row.toSeq)((s, c) => {
            s.updated(c._2, c._3.get())
          }).toArray
        }
        else {
          cols.foldLeft(row.toSeq)((s, c) => {
            s :+ c._3.get()
          }).toArray
        }
        val r: Row = new GenericRowWithSchema(data, structType)
        r
      })
      dicRedis.close()

      res.toIterator
    }

    df.mapPartitions(func)
  }


}
