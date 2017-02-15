package gr.unipi.datacron.queries.operators

import gr.unipi.datacron.store.ExpData
import org.apache.spark.rdd.RDD
import gr.unipi.datacron.common._
import gr.unipi.datacron.encoding._

object CompositeKeyOperators {
  
  def filterBySpatiotemporalInfo(rdd: RDD[String], constraints: SpatioTemporalConstraints, encoder: SimpleEncoder, data: ExpData): RDD[((Int, Long), String)] = {
    val intervalIds = data.temporalGrid.getIntervalIds(constraints)
    val spatialIds = data.spatialGrid.getSpatialIds(constraints)
    
    val result = rdd.map(x => {
      val id = x.substring(0, x.indexOf(Consts.tripleFieldsSeparator)).toLong
      val components = encoder.decodeComponentsFromKey(id)
      //println(components)
      
      //Possible key values:
      // -1: pruned by either temporal or spatial
      //  0: definitely a result triple (does not need refinement)
      //  1: needs only temporal refinement
      //  2: needs only spatial refinement
      //  3: needs both temporal and spatial refinement
      var key = -1
      if ((components._1 >= intervalIds._1) && (components._1 <= intervalIds._2)) {
        //not pruned by temporal
        val sp = spatialIds.get(components._2)
        if (sp.nonEmpty) {
          //not pruned by spatial
          key = 3  //initially set to need both refinements
          if ((components._1 > intervalIds._1) && (components._1 < intervalIds._2)) {
            //does not need temporal refinement
            key -= 1
          }
          if (!sp.get) {
            //does not need spatial refinement
            key -= 2
          }
        }
      }
      
      ((key, id), x)
    }).filter(_._1._1 != -1)
    return result
  }
}