"""
Copyright 2020 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

__author__ = ['davidharcombe@google.com (David Harcombe)']

from googleapiclient.discovery import Resource
from classes.sa360_report_validation.sa360_field_validator import SA360Validator


class Keyword(SA360Validator):

  def __init__(self,
               sa360_service: Resource = None,
               agency: int = None,
               advertiser: int = None) -> None:
    super().__init__(sa360_service, agency, advertiser)
    self.fields = [
      "status",
      "engineStatus",
      "creationTimestamp",
      "lastModifiedTimestamp",
      "agency",
      "agencyId",
      "advertiser",
      "advertiserId",
      "account",
      "accountId",
      "accountEngineId",
      "accountType",
      "campaign",
      "campaignId",
      "campaignStatus",
      "adGroup",
      "adGroupId",
      "adGroupStatus",
      "keywordId",
      "keywordMatchType",
      "keywordText",
      "keywordEngineId",
      "keywordMaxCpc",
      "effectiveKeywordMaxCpc",
      "keywordLandingPage",
      "keywordClickserverUrl",
      "isDisplayKeyword",
      "keywordMaxBid",
      "keywordMinBid",
      "keywordUrlParams",
      "bingKeywordParam2",
      "bingKeywordParam3",
      "keywordLabels",
      "qualityScoreCurrent",
      "topOfPageBidCurrent",
      "effectiveBidStrategyId",
      "effectiveBidStrategy",
      "bidStrategyInherited",
      "effectiveLabels",
      "dfaActions",
      "dfaRevenue",
      "dfaTransactions",
      "dfaWeightedActions",
      "dfaActionsCrossEnv",
      "dfaRevenueCrossEnv",
      "dfaTransactionsCrossEnv",
      "dfaWeightedActionsCrossEnv",
      "avgCpc",
      "avgCpm",
      "avgPos",
      "clicks",
      "cost",
      "ctr",
      "impr",
      "adWordsConversions",
      "adWordsConversionValue",
      "adWordsViewThroughConversions",
      "visits",
      "qualityScoreAvg",
      "topOfPageBidAvg",
      "date",
      "monthStart",
      "monthEnd",
      "quarterStart",
      "quarterEnd",
      "weekStart",
      "weekEnd",
      "yearStart",
      "yearEnd",
      "deviceSegment",
      "floodlightGroup",
      "floodlightGroupId",
      "floodlightGroupTag",
      "floodlightActivity",
      "floodlightActivityId",
      "floodlightActivityTag",
      "ad",
      "adId",
      "isUnattributedAd",
      "adHeadline",
      "adHeadline2",
      "adHeadline3",
      "adDescription1",
      "adDescription2",
      "adDisplayUrl",
      "adLandingPage",
      "adType",
      "adPromotionLine",
    ]