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

from classes.sa360_report_validation.sa360_field_validator import SA360Validator
from classes.sa360_report_validation.visit import Visit
from classes.sa360_report_validation.product_target import ProductTarget
from classes.sa360_report_validation.product_lead_and_cross_sell import ProductLeadAndCrossSell
from classes.sa360_report_validation.product_group import ProductGroup
from classes.sa360_report_validation.product_advertised import ProductAdvertised
from classes.sa360_report_validation.paid_and_organic import PaidAndOrganic
from classes.sa360_report_validation.negative_campaign_target import NegativeCampaignTarget
from classes.sa360_report_validation.negative_campaign_keyword import NegativeCampaignKeyword
from classes.sa360_report_validation.negative_ad_group_target import NegativeAdGroupTarget
from classes.sa360_report_validation.negative_ad_group_keyword import NegativeAdGroupKeyword
from classes.sa360_report_validation.floodlight_activity import FloodlightActivity
from classes.sa360_report_validation.keyword import Keyword
from classes.sa360_report_validation.feed_item import FeedItem
from classes.sa360_report_validation.conversion import Conversion
from classes.sa360_report_validation.campaign_target import CampaignTarget
from classes.sa360_report_validation.bid_strategy import BidStrategy
from classes.sa360_report_validation.ad_group_target import AdGroupTarget
from classes.sa360_report_validation.ad_group import AdGroup
from classes.sa360_report_validation.advertiser import Advertiser
from classes.sa360_report_validation.ad import Ad
from classes.sa360_report_validation.account import Account
from classes.sa360_report_validation.campaign import Campaign

from googleapiclient.discovery import Resource


class SA360ValidatorFactory(object):
  validators = {
    'account': Account,
    'ad': Ad,
    'advertiser': Advertiser,
    'adGroup': AdGroup,
    'adGroupTarget': AdGroupTarget,
    'bidStrategy': BidStrategy,
    'campaign': Campaign,
    'campaignTarget': CampaignTarget,
    'conversion': Conversion,
    'feedItem': FeedItem,
    'floodlightActivity': FloodlightActivity,
    'keyword': Keyword,
    'negativeAdGroupKeyword': NegativeAdGroupKeyword,
    'negativeAdGroupTarget': NegativeAdGroupTarget,
    'negativeCampaignKeyword': NegativeCampaignKeyword,
    'negativeCampaignTarget': NegativeCampaignTarget,
    'paidAndOrganic': PaidAndOrganic,
    'productAdvertised': ProductAdvertised,
    'productGroup': ProductGroup,
    'productLeadAndCrossSell': ProductLeadAndCrossSell,
    'productTarget': ProductTarget,
    'visit': Visit,
  }

  def get_validator(self, report_type: str, sa360_service: Resource,
                    agency: int, advertiser: int) -> SA360Validator:

    validator = self.validators.get(report_type)
    if validator:
      return validator(sa360_service=sa360_service,
                       agency=agency,
                       advertiser=advertiser)

    else:
      raise Exception(f'Unknown report type {report_type}. No validator found.')
