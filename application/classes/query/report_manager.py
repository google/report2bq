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

from classes.query import query
from classes import report_manager


class ManagerInput(query.Query):
  def __init__(self,
               config: report_manager.ManagerConfiguration) -> ManagerInput:
    super().__init__(project=config.project, dataset=config.dataset,
                     query=f"""
SELECT
  *
FROM
  `{config.project}.{config.dataset}.{config.table}` AS Inputs;
""")


class ManagerUpdate(query.Query):
  def __init__(self,
               config: report_manager.ManagerConfiguration) -> ManagerInput:
    super().__init__(project=config.project, dataset=config.dataset,
                     query=f"""
CREATE TABLE IF NOT EXISTS
  `{config.project}.{config.dataset}.{config.table}_processed` AS
SELECT
    minute,
    description,
    advertiserName,
    agencyName,
    dest_dataset,
    offset,
    RevenueMetric,
    notifier,
    ConversionMetric,
    timezone,
    AdvertiserId,
    lookback,
    report,
    email,
    AgencyId,
    last_updated,
    processed
FROM
    `galvanic-card-234919.report2bq_admin.sa360_definition`
WHERE
    1 = 0;

INSERT INTO TABLE
  `{config.project}.{config.dataset}.{config.table}_processed` (
    minute,
    description,
    advertiserName,
    agencyName,
    dest_dataset,
    offset,
    RevenueMetric.type,
    RevenueMetric.value,
    notifier.message,
    notifier.topic,
    ConversionMetric.type,
    ConversionMetric.value,
    timezone,
    AdvertiserId,
    lookback,
    report,
    email,
    AgencyId,
    last_updated,
    processed)
  SELECT
    minute,
    description,
    advertiserName,
    agencyName,
    dest_dataset,
    offset,
    RevenueMetric.type,
    RevenueMetric.value,
    notifier.message,
    notifier.topic,
    ConversionMetric.type,
    ConversionMetric.value,
    timezone,
    AdvertiserId,
    lookback,
    report,
    email,
    AgencyId,
    last_updated,
    CURRENT_TIMESTAMP() AS processed
FROM
  `{config.project}.{config.dataset}.{config.table}`;

TRUNCATE TABLE `{config.project}.{config.dataset}.{config.table}`;
""")
