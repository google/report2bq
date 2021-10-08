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
      minute,
      description,
      country_code,
      advertiserName,
      agency_name AS agencyName,
      dest_dataset,
      offset,
      STRUCT(
        revenue_metric_type AS type,
        revenue_metric_value AS value
      ) AS RevenueMetric,
      STRUCT(
        notifier_message AS message,
        notifier_topic AS topic
      ) AS notifier,
      STRUCT(
        conversion_metric_type AS type,
        conversion_metric_value AS value
      ) AS ConversionMetric,
      timezone,
      SAFE_CAST(agency_id AS STRING) AS AgencyId,
      SAFE_CAST(advertiser_id AS STRING) AS AdvertiserId,
      lookback,
      'sa360_hourly_depleted' AS report,
      email,
      last_updated,
      processed
    FROM
      `{config.project}.{config.dataset}.{config.table}` AS Inputs
    ORDER BY
      last_updated ASC;
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
      country_code,
      advertiserName,
      agency_name,
      dest_dataset,
      offset,
      revenue_metric_type,
      revenue_metric_value,
      notifier_message,
      notifier_topic,
      conversion_metric_type,
      conversion_metric_value,
      timezone,
      advertiser_id,
      lookback,
      report,
      email,
      agency_id,
      last_updated,
      processed
    FROM
      `{config.project}.{config.dataset}.{config.table}`
    WHERE
      FALSE;

    INSERT INTO
      `{config.project}.{config.dataset}.{config.table}_processed` (
      minute,
      description,
      country_code,
      advertiserName,
      agency_name,
      dest_dataset,
      offset,
      revenue_metric_type,
      revenue_metric_value,
      notifier_message,
      notifier_topic,
      conversion_metric_type,
      conversion_metric_value,
      timezone,
      advertiser_id,
      lookback,
      report,
      email,
      agency_id,
      last_updated,
      processed
    )
      SELECT
        minute,
        description,
        country_code,
        advertiserName,
        agency_name,
        dest_dataset,
        offset,
        revenue_metric_type,
        revenue_metric_value,
        notifier_message,
        notifier_topic,
        conversion_metric_type,
        conversion_metric_value,
        timezone,
        advertiser_id,
        lookback,
        report,
        email,
        agency_id,
        last_updated,
        CURRENT_TIMESTAMP() AS processed
    FROM
      `{config.project}.{config.dataset}.{config.table}`;

    TRUNCATE TABLE `{config.project}.{config.dataset}.{config.table}`;
    """)
