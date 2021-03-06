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


class Conversion(SA360Validator):

  def __init__(self,
               sa360_service: Resource = None,
               agency: int = None,
               advertiser: int = None) -> None:
    super().__init__(sa360_service, agency, advertiser)
    self.fields = [
      "status",
      "deviceSegment",
      "floodlightGroup",
      "floodlightGroupConversionType",
      "floodlightGroupId",
      "floodlightGroupTag",
      "floodlightActivity",
      "floodlightActivityId",
      "floodlightActivityTag",
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
      "productTargetId",
      "productGroupId",
      "ad",
      "adId",
      "isUnattributedAd",
      "inventoryAccountId",
      "productId",
      "productCountry",
      "productLanguage",
      "productStoreId",
      "productChannel",
      "conversionId",
      "advertiserConversionId",
      "conversionType",
      "conversionRevenue",
      "conversionQuantity",
      "conversionDate",
      "conversionTimestamp",
      "conversionLastModifiedTimestamp",
      "conversionAttributionType",
      "conversionVisitId",
      "conversionVisitTimestamp",
      "conversionVisitExternalClickId",
      "conversionSearchTerm",
      "floodlightOriginalRevenue",
      "floodlightEventRequestString",
      "floodlightReferrer",
      "floodlightOrderId",
      "feedItemId",
      "feedId",
      "feedType",
    ]
