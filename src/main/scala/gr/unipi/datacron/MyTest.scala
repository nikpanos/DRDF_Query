package gr.unipi.datacron

import java.text.SimpleDateFormat

import gr.unipi.datacron.store.DataStore

object MyTest {

  def main(args : Array[String]): Unit = {
    val spark = DataStore.spark
    val sc = DataStore.sc

    import spark.implicits._
    val df1 = sc.textFile("/unipi_datasets/aviation/adsb/propertyTables/text/property/ADSB_weather_encoded.properties").map(x => {
      x.split(" ")(0).toLong
    }).toDF("sub1")
    val df2 = sc.textFile("/unipi_datasets/aviation/adsb/propertyTables/text/property/LD_maskON_3.5output_encoded.properties").map(x => {
      x.split(" ")(0).toLong
    }).toDF("sub2")
    val df3 = df1.join(df2, df1("sub1")===df2("sub2"), "outer")
    df3.count()
  }
}
