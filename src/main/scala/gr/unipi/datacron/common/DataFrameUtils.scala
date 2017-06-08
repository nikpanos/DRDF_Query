package gr.unipi.datacron.common

object DataFrameUtils {
  implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def hasColumn(colName: String) = df.columns.contains(colName)
  }
}