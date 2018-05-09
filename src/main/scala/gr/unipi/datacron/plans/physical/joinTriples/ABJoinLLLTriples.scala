package gr.unipi.datacron.plans.physical.joinTriples

import gr.unipi.datacron.plans.physical.BasePhysicalPlan
import gr.unipi.datacron.plans.physical.traits.{TJoinTriples, joinDataframesParams}
import gr.unipi.datacron.store.DataStore
import org.apache.spark.sql.DataFrame

case class ABJoinLLLTriples() extends BasePhysicalPlan with TJoinTriples {

  override def joinDataframes(params: joinDataframesParams): DataFrame = throw new Exception("Not implemented!")
}
