package gr.unipi.datacron.common

import scala.language.implicitConversions
import scala.util.matching.Regex

object RegexUtils {

  class RichRegex(underlying: Regex) {
    def matches(s: String): Boolean = underlying.pattern.matcher(s).matches
  }

  implicit def regexToRichRegex(r: Regex): RichRegex = new RichRegex(r)
}