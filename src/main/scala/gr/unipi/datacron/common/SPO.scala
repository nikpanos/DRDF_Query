package gr.unipi.datacron.common

import scala.util.matching.Regex

class SPO(_s: String, _p: String, _o: String) {
  
  val s: String = _s
  val p: String = _p
  val o: String = _o
  
  def canEqual(a: Any) = a.isInstanceOf[SPO]
  
  override def equals(that: Any): Boolean = that match {
    case that: SPO => that.canEqual(this) && this.hashCode == that.hashCode
    case _ => false
  }
  
  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (s == null) 0 else s.hashCode)
    result = prime * result + (if (p == null) 0 else p.hashCode)
    result = prime * result + (if (o == null) 0 else o.hashCode)
    return result
  }
  
  def getRegExpString(): String = {
    var result: String = ""
    result += (if (s == "null") "^-?\\d+" else s)
    result += Consts.tripleFieldsSeparator
    result += (if (p == "null") "-?\\d+" else p)
    result += Consts.tripleFieldsSeparator
    result += (if (o == "null") "-?\\d+" else o)
    return result
  }
}