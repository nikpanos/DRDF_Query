package gr.unipi.datacron.store

import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import gr.unipi.datacron.common.schema.SemanticObject
import org.apache.spark.sql.{DataFrame, Row}

private[store] class TriplesData() {
  import DataStore.spark.implicits._

  private[store] val triplesPath: String = if (AppConfig.yarnMode) {
    AppConfig.getString(qfpNamenode) + AppConfig.getString(qfpHdfsPrefix) + AppConfig.getString(qfpTriplesPath)
  }
  else {
    AppConfig.getString(qfpTriplesPath)
  }
  
  private[store] val data: DataFrame = Benchmarks.doBenchmark[DataFrame](() => {AppConfig.getString(qfpParseTriples) match {
    case Consts.parseParquet => DataStore.spark.read.parquet(triplesPath)
    case Consts.parseString => DataStore.spark.read.text(triplesPath).toDF(tripleSpoStrField)
    case Consts.parseTripleLong => DataStore.spark.read.text(triplesPath).map(s => {
      val tokenizer = TriplesTokenizer(s.getString(0))

      val sub = tokenizer.getNextToken
      val pred = tokenizer.getNextToken
      val obj = tokenizer.getNextToken

      (sub.get, pred.get, obj.get)
      }).toDF(tripleSubLongField, triplePredLongField, tripleObjLongField)
    case Consts.parseTripleLongProperties =>
      import DataStore.spark.implicits._
      //DataStore.sc.textFile(triplesPath).flatMap(_.split(tripleFieldsSeparator).drop(1).sliding(1, 2).flatten.map(_.toLong))}//.distinct().collect().sorted

       val predicates = DataStore.sc.textFile(triplesPath).flatMap(_.split(tripleFieldsSeparator).drop(1).sliding(1, 2).flatten.map(_.toLong)).distinct().collect().sorted

      //val encodedTripleSubLongField = DataStore.dictionaryRedis.getEncodedValue(tripleSubLongField)

      val schema = Array(tripleSubLongField) ++ predicates.map(_.toString)

      println("Found the following schema:")
      predicates.foreach(x => print(x + ", "))
      println("\n Total columns: " + predicates.length)

      if (predicates.length != 8) {
        throw new Exception("Expected 8 columns in dataset")
      }

      DataStore.sc.textFile(triplesPath).map(s => {
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
      }).toDF(schema:_*)
    case _ => throw new Exception("Triples parsing setting not found")
  }}, new BaseOperatorParams() {
    override def operationName: Option[String] = Some("Load triples dataset")
  })

  println("Triples dataset: " + triplesPath)
}

