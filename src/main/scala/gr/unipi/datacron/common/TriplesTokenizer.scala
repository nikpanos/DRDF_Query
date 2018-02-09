package gr.unipi.datacron.common

import gr.unipi.datacron.common.Consts.tripleFieldsSeparator

case class TriplesTokenizer(line: String) {
  var nextPos: Option[Int] = Some(0)

  def getNextToken: Option[Long] = {
    if (nextPos.isEmpty) return None
    val pos = line.indexOf(tripleFieldsSeparator, nextPos.get)
    if (pos >= 0) {
      nextPos = Some(pos + 1)
      Some(line.substring(nextPos.get, pos).toLong)
    }
    else {
      nextPos = None
      Some(line.substring(nextPos.get).toLong)
    }
  }

  def getRestSubstring: Option[String] = {
    if (nextPos.isEmpty) None
    else Some(line.substring(nextPos.get))
  }
}
