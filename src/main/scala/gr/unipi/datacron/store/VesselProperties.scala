package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.schema.SemanticObject
import gr.unipi.datacron.common.{AppConfig, Consts, TriplesTokenizer}
import org.apache.spark.sql.DataFrame

private[store] class VesselProperties() extends BaseHDFSStore {

  import DataStore.spark.implicits._

  protected def configPropertyForDataPath: String = qfpVesselsPath

  protected def datasetName: String = "Vessel Properties"

  protected def readDataset(): DataFrame = AppConfig.getString(qfpParseVessel) match {
    case Consts.parseParquet => DataStore.spark.read.parquet(dataPath)
    case Consts.parseLong =>
      val predicates = DataStore.sc.textFile(dataPath).flatMap(_.split(tripleFieldsSeparator).drop(1).sliding(1, 2).flatten.map(_.toLong)).distinct().collect().sorted

      val schema = Array(tripleSubLongField) ++ predicates.map(_.toString)

      println("Found the following schema:")
      predicates.foreach(x => print(x + ", "))
      println("\n Total columns: " + predicates.length)

      if (predicates.length != 8) {
        throw new Exception("Expected 8 columns in dataset")
      }

      DataStore.sc.textFile(dataPath).map(s => {
        SemanticObject.predicates = predicates
        val tokenizer = TriplesTokenizer(s)

        val sub = tokenizer.getNextToken.get
        val semObject = new SemanticObject(sub)

        var pred: Option[Long] = tokenizer.getNextToken
        var obj: Option[Long] = tokenizer.getNextToken
        while (pred.isDefined) {
          semObject.setPropertyValue(pred.get, obj.get)
          pred = tokenizer.getNextToken
          obj = tokenizer.getNextToken
        }
        semObject.getValues match {
          case Array(a, b, c, d, e, f, g, h) => (semObject.subj, a, b, c, d, e, f, g, h)
        }
      }).toDF(schema: _*)
  }
}
