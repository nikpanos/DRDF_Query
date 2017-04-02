package gr.unipi.datacron.operators.dictionary

import gr.unipi.datacron.operators.BaseOperator
import gr.unipi.datacron.operators.traits.TDictionary
import gr.unipi.datacron.store.DataStore
import gr.unipi.datacron.common.Consts._
import org.apache.spark.sql.DataFrame
import scala.util.Try

case class LSDictionary() extends BaseOperator with TDictionary {
  //import DataStore.spark.implicits._
  
  
  def pointSearchValue(df: DataFrame, key: Long): Option[String] =
    Try(df.filter(df(dicKeyLongField) === key).first().getAs[String](dicValueStrField)).toOption
  
  def pointSearchKey(df: DataFrame, value: String): Option[Long] =
    Try(df.filter(df(dicValueStrField) === value).first().getAs[Long](dicKeyLongField)).toOption
}