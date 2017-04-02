package gr.unipi.datacron.operators

import com.typesafe.config.Config
import gr.unipi.datacron.operators.traits._
import gr.unipi.datacron.operators.dictionary._
import gr.unipi.datacron.operators.triples._
import gr.unipi.datacron.operators.joinDictionaryTriples._

object Executor {
  private var config: Config = _
  
  def init(_config: Config) = {
    config = _config
  }
  
  lazy val dictionary: TDictionary = LSDictionary()
  lazy val triples: TTriples = STriples()
  lazy val joinDictionaryTriples: TJoinDictionaryTriples = MBJoinLSDictionarySTriples()
}