package gr.unipi.datacron.encoding

import gr.unipi.datacron.common._

class SimpleEncoder() extends Serializable {
  val bitsId: Long = AppConfig.getLong(Consts.qfpIDsBits)
  val bitsSpatial: Long = AppConfig.getLong(Consts.qfpSpatialBits)
  val bitsTotal: Long = AppConfig.getLong(Consts.qfpTotalBits)
  
  def decodeComponentsFromKey(key: Long): (Long, Long, Long) = {
    val index_bit_val = math.pow(2, bitsId).longValue - 1L
		val index_val = index_bit_val & key

		val hilbert_bit_val = math.pow(2, bitsId + bitsSpatial).longValue - 1L - index_bit_val
		val hilbert_index = (key & hilbert_bit_val) >> bitsId

		val time_bit_val = math.pow(2, bitsTotal).longValue - 1 - index_bit_val - hilbert_bit_val
		val time_val = (key & time_bit_val) >> (bitsId + bitsSpatial)
		
		//EncodingDetails new_tuple = new EncodingDetails(,hilbert_index>>this.bits_id,index_val,0);
		(time_val, hilbert_index, index_val)
  }
}