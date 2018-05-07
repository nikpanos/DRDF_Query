package gr.unipi.datacron.common

import org.apache.spark.sql.DataFrame

object DataFrameUtils {
  implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def hasColumn(colName: String) = df.columns.contains(colName)
    def hasColumns(colNames: Array[String]) = colNames.forall(df.columns.contains(_))
    def prefixColumns(prefix: String): DataFrame = {
      val renamedColumns = df.columns.map(c=> df(c).as(s"$prefix$c"))
      df.select(renamedColumns: _*)
    }
  }

  def sanitize(input: String): String = s"`$input`"
}