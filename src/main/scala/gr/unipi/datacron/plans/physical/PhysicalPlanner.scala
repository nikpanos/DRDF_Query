package gr.unipi.datacron.plans.physical

import gr.unipi.datacron.common.{Consts, SpatioTemporalRange, AppConfig}
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.encoding.SimpleEncoder
import gr.unipi.datacron.plans.physical.dictionary._
import gr.unipi.datacron.plans.physical.joinTriples._
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.plans.physical.triples._
import org.apache.spark.sql.DataFrame

object PhysicalPlanner extends TTriples with TDictionary with TJoinTriples{

  private def pickTriplesPlanBasedOnRules: TTriples = AppConfig.getString(qfpTriples_trait) match {
      case Consts.tSTriples => STriples()
      case Consts.tLLLTriples => LLLTriples()
      case _ => throw new Exception("Triples trait not found")
    }

  private def pickJoinTriplesPlanBasedOnRules: TJoinTriples = AppConfig.getString(qfpJoinTriples_trait) match {
      case Consts.tMBJoinSTriples => MBJoinSTriples()
      case Consts.tMBJoinLLLTriples => MBJoinLLLTriples()
      case _ => throw new Exception("JoinTriples trait not found")
    }

  private def pickDictionaryPlanBasedOnRules: TDictionary = AppConfig.getString(qfpDictionaryTrait) match {
      case Consts.tLSDictionary => LSDictionary()
      case Consts.tSDictionary => SDictionary()
      case Consts.tRedisDictionary => RdsDictionary()
      case _ => throw new Exception("Dictionary trait not found")
    }

  override def filterByPO(df: DataFrame, pred: Option[Long], obj: Option[Long]): DataFrame = {
    pickTriplesPlanBasedOnRules.filterByPO(df, pred, obj)
  }

  override def filterByColumn(df: DataFrame, columnName: String, value: Any): DataFrame = {
    pickTriplesPlanBasedOnRules.filterByColumn(df, columnName, value)
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

  override def pointSearchValue(key: Long): Option[String] = {
    pickDictionaryPlanBasedOnRules.pointSearchValue(key)
  }

  override def pointSearchKey(value: String): Option[Long] = {
    pickDictionaryPlanBasedOnRules.pointSearchKey(value)
  }

  override def translateColumn(dfTriples: DataFrame, columnName: String): DataFrame = {
    pickDictionaryPlanBasedOnRules.translateColumn(dfTriples, columnName)
  }

  override def translateColumns(dfTriples: DataFrame, columnNames: Array[String]): DataFrame = {
    pickDictionaryPlanBasedOnRules.translateColumns(dfTriples, columnNames)
  }

  override def joinNewObjects(df: DataFrame, dfTriples: DataFrame, subjectColumn: String,  predicates: Map[Long, String]): DataFrame = {
    pickJoinTriplesPlanBasedOnRules.joinNewObjects(df, dfTriples, subjectColumn, predicates)
  }
}
