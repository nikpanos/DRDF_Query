package gr.unipi.datacron.plans.physical

import gr.unipi.datacron.common._
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.plans.physical.dictionary._
import gr.unipi.datacron.plans.physical.joinTriples.{AJoinLLLTriples, _}
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.plans.physical.triples._
import gr.unipi.datacron.common.Benchmarks.doBenchmark
import gr.unipi.datacron.plans.physical.properties.Properties
import org.apache.spark.sql.DataFrame


object PhysicalPlanner extends TTriples with TDictionary with TJoinTriples with TProperties {

  private lazy val sTriples = STriples()
  private lazy val lllTriples = LLLTriples()
  private lazy val mbJoinSTriples = MBJoinSTriples()
  private lazy val mbJoinLLLTriples = MBJoinLLLTriples()
  private lazy val lsDictionary = LSDictionary()
  private lazy val sDictionary = SDictionary()
  private lazy val rdsDictionary = RdsDictionary()
  private lazy val rdsBatchDictionary = RdsBatchDictionary()
  private lazy val aJoinLLLTriples = AJoinLLLTriples()
  private lazy val abJoinLLLTriples = ABJoinLLLTriples()
  private lazy val properties = Properties()

  private def pickTriplesPlanBasedOnRules: TTriples = AppConfig.getString(qfpTriples_trait) match {
      case Consts.tSTriples => sTriples
      case Consts.tLLLTriples => lllTriples
      case _ => throw new Exception("Triples trait not found")
    }

  private def pickJoinTriplesPlanBasedOnRules: TJoinTriples = AppConfig.getString(qfpJoinTriples_trait) match {
      case Consts.tMBJoinSTriples => mbJoinSTriples
      case Consts.tMBJoinLLLTriples => mbJoinLLLTriples
      case Consts.tAJoinLLLTriples => aJoinLLLTriples
      case Consts.tABJoinLLLTriples => abJoinLLLTriples
      case _ => throw new Exception("JoinTriples trait not found")
    }

  private def pickDictionaryPlanBasedOnRules: TDictionary = AppConfig.getString(qfpDictionaryTrait) match {
      case Consts.tLSDictionary => lsDictionary
      case Consts.tSDictionary => sDictionary
      case Consts.tRedisDictionary => rdsDictionary
      case Consts.tRedisBatchDictionary => rdsBatchDictionary
      case _ => throw new Exception("Dictionary trait not found")
    }

  override def filterByPO(params: filterByPOParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByPO(params), params)

  override def filterByPOandKeepSpatioTemporal(params: filterByPOandKeepSpatioTemporalParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByPOandKeepSpatioTemporal(params), params)

  override def filterByColumn(params: filterByColumnParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByColumn(params), params)

  override def filterByPredicateAndRenameObject(params: filterByPredicateAndRenameObjectParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByPredicateAndRenameObject(params), params)

  override def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterBySubSpatioTemporalInfo(params), params)

  override def filterbySpatioTemporalRange(params: filterbySpatioTemporalRangeParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterbySpatioTemporalRange(params), params)

  override def pointSearchObject(params: pointSearchObjectParams): Option[Long] =
    doBenchmark[Option[Long]](() => pickTriplesPlanBasedOnRules.pointSearchObject(params), params)

  override def prepareForFinalTranslation(params: prepareForFinalTranslationParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.prepareForFinalTranslation(params), params)

  override def pointSearchValue(params: pointSearchValueParams): Option[String] =
    doBenchmark[Option[String]](() => pickDictionaryPlanBasedOnRules.pointSearchValue(params), params)

  override def pointSearchKey(params: pointSearchKeyParams): Option[Long] =
    doBenchmark[Option[Long]](() => pickDictionaryPlanBasedOnRules.pointSearchKey(params), params)

  override def decodeColumn(params: decodeColumnParams): DataFrame =
    doBenchmark[DataFrame](() => pickDictionaryPlanBasedOnRules.decodeColumn(params), params)

  override def decodeColumns(params: decodeColumnsParams): DataFrame =
    doBenchmark[DataFrame](() => pickDictionaryPlanBasedOnRules.decodeColumns(params), params)

  override def joinNewObjects(params: joinNewObjectsParams): DataFrame =
    doBenchmark[DataFrame](() => pickJoinTriplesPlanBasedOnRules.joinNewObjects(params), params)

  override def joinDataframes(params: joinDataframesParams): DataFrame =
    doBenchmark[DataFrame](() => pickJoinTriplesPlanBasedOnRules.joinDataframes(params), params)

  override def addTemporaryColumnForRefinement(params: addTemporaryColumnForRefinementParams): DataFrame =
    doBenchmark[DataFrame](() => properties.addTemporaryColumnForRefinement(params), params)

  override def filterStarByTemporaryColumn(params: filterStarByTemporaryColumnParams): DataFrame =
    doBenchmark[DataFrame](() => properties.filterStarByTemporaryColumn(params), params)

  override def addSpatialAndTemporalColumnsByTemporaryColumn(params: addSpatialAndTemporalColumnsByTemporaryColumnParams): DataFrame =
    doBenchmark[DataFrame](() => properties.addSpatialAndTemporalColumnsByTemporaryColumn(params), params)

  override def filterNullProperties(params: filterNullPropertiesParams): DataFrame =
    doBenchmark[DataFrame](() => properties.filterNullProperties(params), params)

  override def filterByProperty(params: filterByPropertyParams): DataFrame =
    doBenchmark[DataFrame](() => properties.filterByProperty(params), params)

  override def addColumnByProperty(params: addColumnByPropertyParams): DataFrame =
    doBenchmark[DataFrame](() => properties.addColumnByProperty(params), params)

  override def addColumnsByProperty(params: addColumnsByPropertyParams): DataFrame =
    doBenchmark[DataFrame](() => properties.addColumnsByProperty(params), params)
}
