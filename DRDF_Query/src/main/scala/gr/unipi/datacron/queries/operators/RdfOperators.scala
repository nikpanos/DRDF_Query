package gr.unipi.datacron.queries.operators

import gr.unipi.datacron.common._
import gr.unipi.datacron.store._
import org.apache.spark.sql.DataFrame

object RdfOperators {
  
  import ExpData.spark.implicits._
  
  def simpleFilter(data: DataFrame, spoFilter: SPO): DataFrame = {
    val searchStr = spoFilter.getRegExpString
    return data.filter($"spo" rlike searchStr)
  }
}