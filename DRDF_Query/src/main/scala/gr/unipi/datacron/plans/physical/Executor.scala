package gr.unipi.datacron.plans.physical

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.dictionary._
import gr.unipi.datacron.plans.physical.joinTriples._
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.plans.physical.triples._

object Executor {
  private var config: Config = _

  def init(_config: Config): Unit = {
    config = _config
  }

  lazy val dictionary: TDictionary = config.getString(qfpDictionaryTrait) match {
    case Consts.tLSDictionary => LSDictionary()
    case Consts.tSDictionary => SDictionary()
    case _ => throw new Exception("Dictionary trait not found")
  }
  lazy val triples: TTriples = config.getString(qfpTriples_trait) match {
    case Consts.tSTriples => STriples()
    case Consts.tLLLTriples => LLLTriples()
    case _ => throw new Exception("Triples trait not found")
  }
  lazy val joinTriples: TJoinTriples = config.getString(qfpJoinTriples_trait) match {
    case Consts.tMBJoinSTriples => MBJoinSTriples()
    case Consts.tMBJoinLLLTriples => MBJoinLLLTriples()
    case _ => throw new Exception("JoinTriples trait not found")
  }
}
