# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import unittest

from datetime import date, datetime, timedelta

from classes import ga360_report as ga360


TODAY = datetime.now()
YESTERDAY = (TODAY - timedelta(days=1)).strftime('%Y-%m-%d')
LAST_WEEK = (TODAY - timedelta(days=7)).strftime('%Y-%m-%d')

class GA360ReportDefinitionTest(unittest.TestCase):
  BASE_EXPECTED = {
    'dimensions': [{'name': 'ga:dimension6'},
                   {'name': 'ga:dcmLastEventAdvertiser'},
                   {'name': 'ga:dcmLastEventSitePlacement'},
                   {'name': 'ga:dcmLastEventSitePlacementId'}],
    'metrics': [{'expression': 'ga:goal13Starts', 'alias': 'Goal 13 Starts'},
                {'expression': 'ga:dbmCost'},
                {'expression': 'ga:dcmCost', 'formattingType': 'CURRENCY'}],
    'viewId': ''}

  DIMENSIONS = [
    "ga:dimension6",
    "ga:dcmLastEventAdvertiser",
    "ga:dcmLastEventSitePlacement",
    "ga:dcmLastEventSitePlacementId"
  ]
  METRICS = [
    ga360.GA360ReportMetric(expression="ga:goal13Starts",
                            alias='Goal 13 Starts'),
    ga360.GA360ReportMetric(expression='ga:dbmCost'),
    ga360.GA360ReportMetric(expression='ga:dcmCost',
                            formatting_type=ga360.GA360MetricType.CURRENCY)
  ]
  maxDiff = None

  def test_good_report_with_known_date_range(self):
    expected = {
      'dateRanges': [
        {'startDate': '2021-03-23', 'endDate': '2021-03-29'}
      ]
    }
    expected.update(self.BASE_EXPECTED)

    definition = \
      ga360.GA360ReportDefinition(view_id='',
                                  metrics=self.METRICS,
                                  date_ranges=[
                                    ga360.GA360DateRange(start_date='2021-03-23',
                                                         end_date='2021-03-29')
                                    ],
                                  dimensions=self.DIMENSIONS)

    self.assertEqual(expected, definition.report_request)

  def test_good_report_with_two_known_date_ranges(self):
    expected = {
      'dateRanges': [
        {'startDate': '2021-03-23', 'endDate': '2021-03-29'},
        {'startDate': '2020-03-23', 'endDate': '2020-03-29'},
      ]
    }
    expected.update(self.BASE_EXPECTED)

    definition = \
      ga360.GA360ReportDefinition(view_id='',
                                  metrics=self.METRICS,
                                  date_ranges=[
                                    ga360.GA360DateRange(start_date='2021-03-23',
                                                         end_date='2021-03-29'),
                                    ga360.GA360DateRange(start_date='2020-03-23',
                                                         end_date='2020-03-29')
                                  ],
                                  dimensions=self.DIMENSIONS)

    self.assertEqual(expected, definition.report_request)

  def test_bad_report_with_more_than_two_date_ranges(self):
    definition = \
      ga360.GA360ReportDefinition(view_id='',
                                  metrics=self.METRICS,
                                  date_ranges=[
                                    ga360.GA360DateRange(),
                                    ga360.GA360DateRange(),
                                    ga360.GA360DateRange()
                                  ],
                                  dimensions=self.DIMENSIONS)

    with self.assertRaises(ValueError):
      definition.report_request

  def test_load_from_json(self):
    expected = {
      'dateRanges': [
        ga360.GA360DateRange().date_range,
      ]
    }
    expected.update(self.BASE_EXPECTED)

    definition = \
      ga360.GA360ReportDefinition.from_json(json.dumps(self.BASE_EXPECTED))
    self.assertEqual(expected, definition.report_request)

  def test_load_from_json_and_edit(self):
    view_id = '00000001'
    date_range = ga360.GA360DateRange(start_date='30daysAgo',
                                      end_date='yesterday')
    updates = {
      'viewId': view_id,
      'dateRanges': [date_range.date_range]
    }
    expected = {}
    expected.update(self.BASE_EXPECTED, **updates)

    definition = \
      ga360.GA360ReportDefinition.from_json(json.dumps(self.BASE_EXPECTED))
    definition.view_id = view_id
    definition.date_ranges = [ date_range ]
    self.assertEqual(expected, definition.report_request)


class GA360DateRangeTest(unittest.TestCase):
  maxDiff = None

  def test_known_date_range(self):
    date_range = \
      ga360.GA360DateRange(start_date='2021-03-23', end_date='2021-03-29')

    self.assertEqual({'startDate': '2021-03-23', 'endDate': '2021-03-29'},
                     date_range.date_range)

  def test_start_date_only(self):
    date_range = \
      ga360.GA360DateRange(start_date='2021-03-23')

    self.assertEqual({'startDate': '2021-03-23', 'endDate': YESTERDAY},
                     date_range.date_range)

  def test_end_date_only(self):
    date_range = \
      ga360.GA360DateRange(end_date='2021-03-29')

    self.assertEqual({'startDate': LAST_WEEK, 'endDate': '2021-03-29'},
                     date_range.date_range)

  def test_no_dates_given(self):
    date_range = \
      ga360.GA360DateRange()

    self.assertEqual({'startDate': LAST_WEEK, 'endDate': YESTERDAY},
                     date_range.date_range)

  def test_date_math(self):
    def mock_ga360_date_range(d):
      d.__setattr__('_lazy__now', date(2021, 4, 1))
      return d

    date_ranges = [
      mock_ga360_date_range(ga360.GA360DateRange(start_date='7daysAgo')),
      mock_ga360_date_range(ga360.GA360DateRange(start_date='14daysAgo')),
      mock_ga360_date_range(ga360.GA360DateRange(start_date='1weeksAgo')),
      mock_ga360_date_range(ga360.GA360DateRange(start_date='1monthsAgo')),
      mock_ga360_date_range(ga360.GA360DateRange(start_date='12MonthsAgo')),
      mock_ga360_date_range(ga360.GA360DateRange(start_date='1yearsago')),
    ]

    self.assertEqual(
      [
        {'startDate': '2021-03-25', 'endDate': '2021-03-31'},
        {'startDate': '2021-03-18', 'endDate': '2021-03-31'},
        {'startDate': '2021-03-25', 'endDate': '2021-03-31'},
        {'startDate': '2021-03-01', 'endDate': '2021-03-31'},
        {'startDate': '2020-04-01', 'endDate': '2021-03-31'},
        {'startDate': '2020-04-01', 'endDate': '2021-03-31'},
      ],
      [ d.date_range for d in date_ranges ])

  def test_invalid_date_math(self):
    with self.assertRaises(NotImplementedError):
      ga360.GA360DateRange(start_date='7hoursAgo').date_range


class GA360MetricTypeTest(unittest.TestCase):
  def test_valid_enum(self):
    self.assertEqual(ga360.GA360MetricType.TIME, ga360.GA360MetricType('TIME'))

  def test_unknown(self):
    self.assertEqual([
                       ga360.GA360MetricType.METRIC_TYPE_UNSPECIFIED,
                       ga360.GA360MetricType.METRIC_TYPE_UNSPECIFIED,
                     ],
                     [ ga360.GA360MetricType(T) for T in ['foo', None] ])
