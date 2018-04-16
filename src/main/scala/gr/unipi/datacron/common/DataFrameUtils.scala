package gr.unipi.datacron.common

object DataFrameUtils {
  implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def hasColumn(colName: String) = df.columns.contains(colName)
    def hasColumns(colNames: Array[String]) = colNames.forall(df.columns.contains(_))
  }
}