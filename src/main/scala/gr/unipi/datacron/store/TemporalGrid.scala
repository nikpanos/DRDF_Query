package gr.unipi.datacron.store

import gr.unipi.datacron.common._

class TemporalGrid() {
  private val redis = DataStore.dictionaryRedis

  def getIntervalIds(c: SpatioTemporalRange): (Int, Int) = (redis.getLowerTimestampIdx(c.low.time), redis.getUpperTimestampIdx(c.high.time))
}