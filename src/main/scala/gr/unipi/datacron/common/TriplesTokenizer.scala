package gr.unipi.datacron.common

import gr.unipi.datacron.common.Consts.tripleFieldsSeparator

case class TriplesTokenizer(line: String) {
  var nextPos: Option[Int] = Some(0)

  def getNextToken: Option[Long] = {
    if (nextPos.isEmpty) return None
    val pos = nextPos.get
    val tmp = line.indexOf(tripleFieldsSeparator, pos)
    nextPos = if (tmp >= 0) Some(tmp + 1)
    else None
    if (nextPos.isDefined) {
      Some(line.substring(pos, nextPos.get - 1).toLong)
    }
    else {
      Some(line.substring(pos).toLong)
    }
  }

  def getRestSubstring: Option[String] = {
    if (nextPos.isEmpty) None
    else Some(line.substring(nextPos.get))
  }
}
