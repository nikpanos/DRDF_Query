package gr.unipi.datacron.common

object TupleCreator {
  def getTuple(subject: Long, values: Array[Long]): Any = {
    values match {
      case Array() => subject
      case Array(a) => (subject, a)
      case Array(a, b) => (subject, a, b)
      case Array(a, b, c) => (subject, a, b, c)
      case Array(a, b, c, d) => (subject, a, b, c, d)
      case Array(a, b, c, d, e) => (subject, a, b, c, d, e)
      case Array(a, b, c, d, e, f) => (subject, a, b, c, d, e, f)
      case Array(a, b, c, d, e, f, g) => (subject, a, b, c, d, e, f, g)
      case Array(a, b, c, d, e, f, g, h) => (subject, a, b, c, d, e, f, g, h)
      case Array(a, b, c, d, e, f, g, h, i) => (subject, a, b, c, d, e, f, g, h, i)
      case Array(a, b, c, d, e, f, g, h, i, j) => (subject, a, b, c, d, e, f, g, h, i, j)
      case Array(a, b, c, d, e, f, g, h, i, j, k) => (subject, a, b, c, d, e, f, g, h, i, j, k)
      case Array(a, b, c, d, e, f, g, h, i, j, k, l) => (subject, a, b, c, d, e, f, g, h, i, j, k, l)
      case Array(a, b, c, d, e, f, g, h, i, j, k, l, m) => (subject, a, b, c, d, e, f, g, h, i, j, k, l, m)
      case Array(a, b, c, d, e, f, g, h, i, j, k, l, m, n) => (subject, a, b, c, d, e, f, g, h, i, j, k, l, m, n)
      case _ => subject
    }
  }
}
