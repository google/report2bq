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

from typing import Any, List, Optional, Union

from auth import credentials as creds
from google.cloud import bigquery
from google.oauth2 import credentials as oauth


class Query():
  project: str = None
  dataset: str = None
  query: str = None
  parameters: List[Union[bigquery.ArrayQueryParameter,
                         bigquery.ScalarQueryParameter]] = []
  columns: List[str] = []
  credentials: Union[creds.Credentials, oauth.Credentials] = None

  def __init__(self,
               project: str,
               dataset: str,
               query: Union[str, None] = None) -> Query:
    self.project = project
    self.dataset = dataset
    self.query = query

  def execute(self,
              params: Optional[List[Any]] = []) -> bigquery.QueryJob:
    if not self.query:
      raise NotImplementedError('No query set!')

    client = bigquery.Client(project=self.project)

    for param, value in list(zip(self.parameters, params)):
      param.values = value

    job_config = bigquery.QueryJobConfig(query_parameters=self.parameters)
    query_job = client.query(self.query.strip(), job_config=job_config)

    return query_job
