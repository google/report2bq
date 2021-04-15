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
import csv
import io
import json
import unittest

from classes import ga360_report_response as ga360


if 'unittest.util' in __import__('sys').modules:
    # Show full diff in self.assertEqual.
    __import__('sys').modules['unittest.util']._MAX_LENGTH = 999999999

BASE_RESPONSE = {
  "columnHeader": {
    "dimensions": [ "ga:dimension6",
                    "ga:dcmLastEventAdvertiser",
                    "ga:dcmLastEventSitePlacement",
                    "ga:dcmLastEventSitePlacementId" ],
    "metricHeader": {
      "metricHeaderEntries": [
          { "name": "Goal 13 Starts", "type": "INTEGER" },
          { "name": "DV360 Cost", "type": "CURRENCY" },
          { "name": "CM360 Cost", "type": "CURRENCY" }
      ] }
  },
  "data": {
    "rows": [
      { "dimensions": [
          "Row 1 dimension6",
          "Row 1 dcmLastEventAdvertiser",
          "Row 1 dcmLastEventSitePlacement",
          "Row 1 dcmLastEventSitePlacementId" ],
        "metrics": [ { "values": [ "1", "1.0", "1.0" ] } ]
      },
      {
        "dimensions": [
          "Row 2 dimension6",
          "Row 2 dcmLastEventAdvertiser",
          "Row 2 dcmLastEventSitePlacement",
          "Row 2 dcmLastEventSitePlacementId" ],
        "metrics": [ { "values": [ "2", "2.0", "2.0" ] }]
      },
      {
        "dimensions": [
          "Row 3 dimension6",
          "Row 3 dcmLastEventAdvertiser",
          "Row 3 dcmLastEventSitePlacement",
          "Row 3 dcmLastEventSitePlacementId" ],
        "metrics": [ { "values": [ "2", "3.0", "3.0" ] } ]
      },
      {
        "dimensions": [
          "Row 4 dimension6",
          "Row 4 dcmLastEventAdvertiser",
          "Row 4 dcmLastEventSitePlacement",
          "Row 4 dcmLastEventSitePlacementId" ],
        "metrics": [ { "values": [ "4", "4.0", "4.0" ] } ]
      },
    ],
    "totals": [ { "values": [ "9", "9.0", "9.0" ] } ],
    "rowCount": 4,
    "minimums": [ { "values": [ "1", "1.0", "1.0" ] } ],
    "maximums": [ { "values": [ "4", "4.0", "4.0" ] } ],
    "samplesReadCounts": [ "999999" ],
    "samplingSpaceSizes": [ "10000000" ]
  }
}


class GA360ReportResponseTest(unittest.TestCase):

  def test_good_response(self):
    response = ga360.GA360ReportResponse.from_json(json.dumps(BASE_RESPONSE))

    self.assertEqual([
      'ga:dimension6', 'ga:dcmLastEventAdvertiser',
      'ga:dcmLastEventSitePlacement', 'ga:dcmLastEventSitePlacementId' ],
      response.column_header.dimensions)
    self.assertEqual(['Goal 13 Starts', 'DV360 Cost', 'CM360 Cost'],
      list([header.name \
        for header in \
          response.column_header.metric_header.metric_header_entries]))
    self.assertEqual(4, len(response.data.rows))
    self.assertEqual(4, response.data.row_count)
    self.assertEqual([ 'Row 4 dimension6', 'Row 4 dcmLastEventAdvertiser',
                       'Row 4 dcmLastEventSitePlacement',
                       'Row 4 dcmLastEventSitePlacementId'],
                     response.data.rows[-1].dimensions)
    self.assertEqual([ "4", "4.0", "4.0" ],
                     list([value \
                       for value in response.data.rows[-1].metrics[0].values ]))

  def test_bad_response(self):
    BAD_RESPONSE = BASE_RESPONSE.copy()
    BAD_RESPONSE.pop('data')
    with self.assertRaisesRegex(KeyError, 'data'):
      ga360.GA360ReportResponse.from_json(json.dumps(BAD_RESPONSE))

  def test_csv(self):
    response = ga360.GA360ReportResponse.from_json(json.dumps(BASE_RESPONSE))
    csv_buffer = io.StringIO()
    response.to_csv(csv_buffer)

    reader = list(csv.reader(csv_buffer.getvalue().splitlines()))
    self.assertEqual(5, len(reader))
    self.assertEqual(['ga:dimension6', 'ga:dcmLastEventAdvertiser',
                      'ga:dcmLastEventSitePlacement',
                      'ga:dcmLastEventSitePlacementId', 'Goal 13 Starts',
                      'DV360 Cost', 'CM360 Cost'],
                     reader[0])
    self.assertEqual(['Row 4 dimension6', 'Row 4 dcmLastEventAdvertiser',
                      'Row 4 dcmLastEventSitePlacement',
                      'Row 4 dcmLastEventSitePlacementId', '4', '4.0', '4.0'],
                     reader[-1])
