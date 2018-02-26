package gr.unipi.datacron

import gr.unipi.datacron.common.TriplesTokenizer
import gr.unipi.datacron.common.schema.SemanticObject

object MyTest {
  def main(args : Array[String]) {
    val s = "5120 -3 -4 -5 -6 -7 -8 -9 -10 -11 -12 -13 -14 -15 -12 -16 -17"
    SemanticObject.predicates = Array(-3, -5, -7, -9, -11, -13, -15, -16)
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
    val test = semObject.getValues match {
      case Array(a, b, c, d, e, f, g, h) => (semObject.subj, a, b, c, d, e, f, g, h)
    }
  }
}
