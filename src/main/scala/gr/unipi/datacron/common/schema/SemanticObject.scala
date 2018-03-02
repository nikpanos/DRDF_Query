package gr.unipi.datacron.common.schema

import scala.collection.immutable.HashMap

class SemanticObject(val subj: Long) {
  var properties = HashMap(SemanticObject.predicates.map(pred => pred -> 0L):_*)

  def setPropertyValue(pred: Long, obje: Long): Unit = {
    if (properties.contains(pred)) properties += (pred -> obje)
    else throw new Exception("Took predicate that was not expected: " + pred)
  }

  def getValues: Array[Long] = properties.toArray.sortBy(_._1).map(_._2)
}

object SemanticObject {
  var predicates: Array[Long] = _
}