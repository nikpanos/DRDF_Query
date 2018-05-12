package gr.unipi.datacron.common

import org.apache.spark.sql.DataFrame
import gr.unipi.datacron.common.Consts._

object DataFrameUtils {
  implicit class DataFrameImprovements(df: org.apache.spark.sql.DataFrame) {
    def hasColumn(colName: String): Boolean = df.columns.contains(colName)
    def hasColumns(colNames: Array[String]): Boolean = colNames.forall(df.columns.contains(_))
    def prefixColumns(prefix: String): DataFrame = {
      val renamedColumns = df.columns.map(c => df(c).as(s"$prefix$c"))
      df.select(renamedColumns: _*)
    }
    def isPrefixed: Boolean = df.columns(0).contains('.')
    def isPropertyTable: Boolean = !df.hasColumn(triplePredLongField)

    def getIncludingColumns(cols: Array[String]): Array[String] = cols.filter(df.columns.contains)
    def getExcludingColumns(cols: Array[String]): Array[String] = cols.filter(!df.columns.contains(_))

    def findColumnNameWithPrefix(colName: String): Option[String] = df.columns.find(_.endsWith(colName))
  }

  def sanitize(input: String): String = s"`$input`"
}