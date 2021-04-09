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
from __future__ import annotations

import csv
import dataclasses
import io
import dataclasses_json

from classes.ga360_report import GA360MetricType

from typing import Any, Dict, List, Optional


@dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
@dataclasses.dataclass
class GA360ReportResponse(object):
  column_header: GA360ReportResponse.ColumnHeader
  data: GA360ReportResponse.ReportData

  def to_csv(self, output: io.StringIO) -> None:
    # Fetch the field names from the column_header
    fieldnames = self.column_header.fieldnames

    # Create csv.DictWriter using this and a buffer
    writer = \
      csv.DictWriter(output, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
    writer.writeheader()

    # Write each data row to the csv.DW
    for row_data in self.data.rows:
      result_row = dict(zip(fieldnames, row_data.row))
      writer.writerow(result_row)

  @dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
  @dataclasses.dataclass
  class ColumnHeader(object):
    dimensions: List[str]
    metric_header: GA360ReportResponse.MetricHeader

    @property
    def fieldnames(self) -> List[str]:
      metric_names = \
        [ header.name for header in self.metric_header.metric_header_entries ]
      fieldnames = [ *self.dimensions.copy(), *metric_names ]
      return fieldnames

  @dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
  @dataclasses.dataclass
  class MetricHeaderEntry(object):
    name: str
    type: GA360MetricType

  @dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
  @dataclasses.dataclass
  class MetricHeader(object):
    metric_header_entries: List[GA360ReportResponse.MetricHeaderEntry]
    pivot_headers: Optional[List[Any]] = None

  @dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
  @dataclasses.dataclass
  class ReportData(object):
    rows: List[GA360ReportResponse.ReportRow]
    totals: List[GA360ReportResponse.DateRangeValues]
    row_count: int
    minimums: List[GA360ReportResponse.DateRangeValues]
    maximums: List[GA360ReportResponse.DateRangeValues]
    samples_read_counts: List[str]
    sampling_space_sizes: List[str]
    is_data_golden: Optional[bool] = None
    data_last_refreshed: Optional[str] = None

  @dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
  @dataclasses.dataclass
  class ReportRow(object):
    dimensions: List[str]
    metrics: List[GA360ReportResponse.DateRangeValues]

    @property
    def row(self) -> List[str]:
      row = [ *self.dimensions, *self.metrics[0].values ]
      return row

  @dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
  @dataclasses.dataclass
  class DateRangeValues(object):
    values: List[str]
    pivot_value_regions: Optional[List[GA360ReportResponse.PivotValueRegion]] = None

  @dataclasses_json.dataclass_json(letter_case=dataclasses_json.LetterCase.CAMEL)
  @dataclasses.dataclass
  class PivotValueRegion(object):
    values: List[str]
