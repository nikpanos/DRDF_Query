package gr.unipi.datacron.encoding

import gr.unipi.datacron.common._

class SimpleEncoder private (val bitsId: Long, val bitsSpatial: Long, val bitsTotal: Long) extends Serializable {
  
  def decodeComponentsFromKey(key: Long): (Long, Long) = {
    val index_bit_val = math.pow(2, bitsId).longValue - 1L
		//val index_val = index_bit_val & key

		val hilbert_bit_val = math.pow(2, bitsId + bitsSpatial).longValue - 1L - index_bit_val
		val hilbert_index = (key & hilbert_bit_val) >> bitsId

		val time_bit_val = math.pow(2, bitsTotal).longValue - 1L - index_bit_val - hilbert_bit_val
		val time_val = (key & time_bit_val) >> (bitsId + bitsSpatial)
		
		//EncodingDetails new_tuple = new EncodingDetails(,hilbert_index>>this.bits_id,index_val,0);
		(time_val, hilbert_index)
  }

  def encodeKeyFromComponents(timePartition: Long, hilbertValue: Long, index: Long): Long = {
    val time_part_val = timePartition << (bitsSpatial + bitsId)
    val hilbert_val = hilbertValue << this.bitsId
    time_part_val + hilbert_val + index
  }
}

object SimpleEncoder {
  def apply(): SimpleEncoder = new SimpleEncoder(
    AppConfig.getLong(Consts.qfpIDsBits),
    AppConfig.getLong(Consts.qfpSpatialBits),
    AppConfig.getLong(Consts.qfpTotalBits))
}