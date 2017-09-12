package gr.unipi.datacron.common

import gr.unipi.datacron.common.Consts.tripleFieldsSeparator

object TriplesParsingUtils {
  def getNextToken(line: String, fromPos: Option[Int]): (Option[Int], Long) = {
    if (fromPos.isEmpty) {
      (None, 0)
    }
    else {
      val pos = line.indexOf(tripleFieldsSeparator, fromPos.get)
      if (pos >= 0) {
        (Some(pos + 1), line.substring(fromPos.get, pos).toLong)
      }
      else {
        (None, line.substring(fromPos.get).toLong)
      }
    }
  }
}
