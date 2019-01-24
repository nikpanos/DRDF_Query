package gr.unipi.datacron.plans.physical

import gr.unipi.datacron.common.Benchmarks.doBenchmark
import gr.unipi.datacron.common.Consts._
import gr.unipi.datacron.common._
import gr.unipi.datacron.plans.physical.dictionary._
import gr.unipi.datacron.plans.physical.joinTriples.{AJoinLLLTriples, _}
import gr.unipi.datacron.plans.physical.projection.Projection
import gr.unipi.datacron.plans.physical.properties.Properties
import gr.unipi.datacron.plans.physical.traits._
import gr.unipi.datacron.plans.physical.triples._
import org.apache.spark.sql.DataFrame


object PhysicalPlanner extends TTriples with TDictionary with TJoinTriples with TProperties with TProjection {

  private lazy val lllTriples = LLLTriples()
  private lazy val mbJoinSTriples = MBJoinSTriples()
  private lazy val mbJoinLLLTriples = MBJoinLLLTriples()
  private lazy val rdsDictionary = RdsDictionary()
  private lazy val rdsBatchDictionary = RdsBatchDictionary()
  private lazy val aJoinLLLTriples = AJoinLLLTriples()
  private lazy val abJoinLLLTriples = ABJoinLLLTriples()
  private lazy val properties = Properties()
  private lazy val projection = Projection()

  private def pickTriplesPlanBasedOnRules: TTriples = AppConfig.getString(qfpTriples_trait) match {
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
    case Consts.tRedisDictionary => rdsDictionary
    case Consts.tRedisBatchDictionary => rdsBatchDictionary
    case _ => throw new Exception("Dictionary trait not found")
  }

  override def filterByColumnValue(params: filterByColumnValueParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByColumnValue(params), params)

  override def filterByColumn(params: filterByColumnParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByColumn(params), params)

  override def filterByLiteralOperandPair(params: filterByLiteralOperandPairParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByLiteralOperandPair(params), params)

  override def filterBySubSpatioTemporalInfo(params: filterBySubSpatioTemporalInfoParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterBySubSpatioTemporalInfo(params), params)

  override def filterBySpatioTemporalRange(params: filterBySpatioTemporalRangeParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterBySpatioTemporalRange(params), params)

  override def filterByMultipleOr(params: filterByMultipleOrParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByMultipleOr(params), params)

  override def unionDataframes(params: unionDataframesParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.unionDataframes(params), params)

  override def decodeSingleKey(params: decodeSingleKeyParams): Option[String] = pickDictionaryPlanBasedOnRules.decodeSingleKey(params)

  override def encodeSingleValue(params: encodeSingleValueParams): Option[Long] = pickDictionaryPlanBasedOnRules.encodeSingleValue(params)

  override def decodeColumn(params: decodeColumnParams): DataFrame =
    doBenchmark[DataFrame](() => pickDictionaryPlanBasedOnRules.decodeColumn(params), params)

  override def decodeColumns(params: decodeColumnsParams): DataFrame =
    doBenchmark[DataFrame](() => pickDictionaryPlanBasedOnRules.decodeColumns(params), params)

  override def decodeAllColumns(params: decodeAllColumnsParams): DataFrame =
    doBenchmark[DataFrame](() => pickDictionaryPlanBasedOnRules.decodeAllColumns(params), params)

  override def decodeAllColumnsExceptFor(params: decodeAllColumnsExceptForParams): DataFrame =
    doBenchmark[DataFrame](() => pickDictionaryPlanBasedOnRules.decodeAllColumnsExceptFor(params), params)

  override def joinDataframes(params: joinDataframesParams): DataFrame =
    doBenchmark[DataFrame](() => pickJoinTriplesPlanBasedOnRules.joinDataframes(params), params)

  override def addTemporaryColumnForRefinement(params: addTemporaryColumnForRefinementParams): DataFrame =
    doBenchmark[DataFrame](() => properties.addTemporaryColumnForRefinement(params), params)

  override def addSpatialAndTemporalColumnsByTemporaryColumn(params: addSpatialAndTemporalColumnsByTemporaryColumnParams): DataFrame =
    doBenchmark[DataFrame](() => properties.addSpatialAndTemporalColumnsByTemporaryColumn(params), params)

  override def filterNullProperties(params: filterNullPropertiesParams): DataFrame =
    doBenchmark[DataFrame](() => properties.filterNullProperties(params), params)

  override def dropColumns(params: dropColumnsParams): DataFrame =
    doBenchmark[DataFrame](() => projection.dropColumns(params), params)

  override def renameColumns(params: renameColumnsParams): DataFrame =
    doBenchmark[DataFrame](() => projection.renameColumns(params), params)

  override def prefixColumns(params: prefixColumnsParams): DataFrame =
    doBenchmark[DataFrame](() => projection.prefixColumns(params), params)

  override def selectColumns(params: selectColumnsParams): DataFrame =
    doBenchmark[DataFrame](() => projection.selectColumns(params), params)

  override def limitResults(params: limitResultsParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.limitResults(params), params)

  override def sortResults(params: sortResultsParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.sortResults(params), params)

  override def filterByValue(params: filterByValueParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.filterByValue(params), params)

  override def distinctData(params: distinctDataParams): DataFrame =
    doBenchmark[DataFrame](() => pickTriplesPlanBasedOnRules.distinctData(params), params)
}
