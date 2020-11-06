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


class FeedItem(SA360Validator):

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
      "sitelinkDisplayText",
      "sitelinkDescription1",
      "sitelinkDescription2",
      "sitelinkLandingPageUrl",
      "sitelinkClickserverUrl",
      "locationBusinessName",
      "locationCategory",
      "locationDetails",
      "callPhoneNumber",
      "callCountryCode",
      "callIsTracked",
      "callCallOnly",
      "callConversionTracker",
      "callConversionTrackerId",
      "appId",
      "appStore",
      "feedItemId",
      "feedId",
      "clicksWithFeedItemShowing",
      "costWithFeedItemShowing",
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
      "date",
      "monthStart",
      "monthEnd",
      "quarterStart",
      "quarterEnd",
      "weekStart",
      "weekEnd",
      "yearStart",
      "yearEnd",
      "floodlightGroup",
      "floodlightGroupId",
      "floodlightGroupTag",
      "floodlightActivity",
      "floodlightActivityId",
      "floodlightActivityTag",
    ]
