package gr.unipi.datacron.operators

import com.typesafe.config.Config
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common.Consts
import gr.unipi.datacron.operators.traits._
import gr.unipi.datacron.operators.dictionary._
import gr.unipi.datacron.operators.triples._
import gr.unipi.datacron.operators.joinTriples._

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
    case _ => throw new Exception("Triples trait not found")
  }
  lazy val joinTriples: TJoinTriples = config.getString(qfpJoinTriples_trait) match {
    case Consts.tMBJoinSTriples => MBJoinSTriples()
    case _ => throw new Exception("JoinTriples trait not found")
  }
}