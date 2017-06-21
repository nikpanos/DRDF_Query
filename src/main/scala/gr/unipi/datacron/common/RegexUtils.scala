package gr.unipi.datacron.common

import scala.util.matching.Regex
import scala.language.implicitConversions

object RegexUtils {
  class RichRegex(underlying: Regex) {
    def matches(s: String): Boolean = underlying.pattern.matcher(s).matches
  }
  implicit def regexToRichRegex(r: Regex): RichRegex = new RichRegex(r)
}