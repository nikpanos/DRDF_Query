package gr.unipi.datacron.plans.physical

import com.typesafe.config.Config
import gr.unipi.datacron.common.{Consts, SpatioTemporalRange}
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.plans.physical.dictionary._
import gr.unipi.datacron.plans.physical.joinTriples._
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.plans.physical.triples._
import org.apache.spark.sql.DataFrame

object PhysicalPlanner extends TTriples with TDictionary with TJoinTriples{
  private var config: Config = _

  def init(_config: Config): Unit = {
    config = _config
  }

  private def pickTriplesPlanBasedOnRules: TTriples = config.getString(qfpTriples_trait) match {
      case Consts.tSTriples => STriples()
      case Consts.tLLLTriples => LLLTriples()
      case _ => throw new Exception("Triples trait not found")
    }

  private def pickJoinTriplesPlanBasedOnRules: TJoinTriples = config.getString(qfpJoinTriples_trait) match {
      case Consts.tMBJoinSTriples => MBJoinSTriples()
      case Consts.tMBJoinLLLTriples => MBJoinLLLTriples()
      case _ => throw new Exception("JoinTriples trait not found")
    }

  private def pickDictionaryPlanBasedOnRules: TDictionary = config.getString(qfpDictionaryTrait) match {
      case Consts.tLSDictionary => LSDictionary()
      case Consts.tSDictionary => SDictionary()
      case _ => throw new Exception("Dictionary trait not found")
    }

  override def filterByPO(df: DataFrame, pred: Option[Long], obj: Option[Long]): DataFrame = {
    pickTriplesPlanBasedOnRules.filterByPO(df, pred, obj)
  }

  override def filterBySubSpatioTemporalInfo(df: DataFrame, constraints: SpatioTemporalRange, encoder: SimpleEncoder): DataFrame = {
    pickTriplesPlanBasedOnRules.filterBySubSpatioTemporalInfo(df, constraints, encoder)
  }

  override def pointSearchObject(df: DataFrame, sub: Long, pred: Long): Option[Long] = {
    pickTriplesPlanBasedOnRules.pointSearchObject(df, sub, pred)
  }

  override def filterbySpatioTemporalRange(df: DataFrame, range: SpatioTemporalRange): DataFrame = {
    pickTriplesPlanBasedOnRules.filterbySpatioTemporalRange(df, range)
  }

  override def prepareForFinalTranslation(df: DataFrame): DataFrame = {
    pickTriplesPlanBasedOnRules.prepareForFinalTranslation(df)
  }

  override def pointSearchValue(df: DataFrame, key: Long): Option[String] = {
    pickDictionaryPlanBasedOnRules.pointSearchValue(df, key)
  }

  override def pointSearchKey(df: DataFrame, value: String): Option[Long] = {
    pickDictionaryPlanBasedOnRules.pointSearchKey(df, value)
  }

  override def translateColumn(dfTriples: DataFrame, dfDictionary: DataFrame, columnName: String): DataFrame = {
    pickDictionaryPlanBasedOnRules.translateColumn(dfTriples, dfDictionary, columnName)
  }

  override def translateColumns(dfTriples: DataFrame, dfDictionary: DataFrame, columnNames: Array[String]): DataFrame = {
    pickDictionaryPlanBasedOnRules.translateColumns(dfTriples, dfDictionary, columnNames)
  }

  override def joinNewObjects(df: DataFrame, dfTriples: DataFrame, subjectColumn: String,  predicates: Map[Long, String]): DataFrame = {
    pickJoinTriplesPlanBasedOnRules.joinNewObjects(df, dfTriples, subjectColumn, predicates)
  }
}
