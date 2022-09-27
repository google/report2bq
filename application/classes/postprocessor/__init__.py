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

__author__ = [
    'davidharcombe@google.com (David Harcombe)'
]

# Python logging
import os
import sys

from importlib import abc
import types
from importlib import machinery
from typing import Any, Dict, Mapping, Optional

from classes.cloud_storage import Cloud_Storage
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.cloud.bigquery import table


class PostProcessorFinder(abc.MetaPathFinder):
  """Check class type

  This class checks to see if the class being loaded is a subclass of
  'PostProcessor'. If it isn't, it won't be loaded.
  """

  def find_spec(self,
                fullname: str,
                path: str,
                target: Optional[str] = None) -> machinery.ModuleSpec:
    """
    Locate the file in GCS. The "spec" should then be
    fullname = fullname
    path = location in GCS, should be hardwired in config
    """
    if 'postprocessor' not in fullname:
      return None                     # we don't handle this this

    else:
      return machinery.ModuleSpec(fullname, PostProcessorLoader())


class PostProcessorLoader(abc.Loader):
  """Load a PostProcessor

  Load an arbitrary PostProcessor subclass into the Python class library
  dynamically. The location to check is hardwired here for security
  reasons.
  """

  def create_module(self, spec: machinery.ModuleSpec):
    return None  # use default module creation semantics

  def exec_module(self, module: types.ModuleType):
    try:
      # Fetch the code here as string:
      # GCS? BQ? Firestore? All good options
      filename = module.__name__.split('.')[-1]
      code = Cloud_Storage.fetch_file(
          bucket=(f'{os.environ.get("GCP_PROJECT")}-report2bq-postprocessor'),
          file=f'{filename}.py'
      )
      exec(code, vars(module))
    except:
      raise ModuleNotFoundError()


class PostProcessor(object):
  """PostProcessor Abstract parent class

  In order to be loaded by the PostProcessor mechanism, all/any classes
  MUST extend this class and implement the 'run' method.
  """

  def install_postprocessor():
    """Inserts the finder into the import machinery"""
    sys.meta_path.append(PostProcessorFinder())

  def check_table_exists(self, project: str, dataset: str, table: str) -> bool:
    """Check if a table exists in BigQuery dataset.

    Commonly needed helper function

    Args:
        project (str): the project where the dataset and table reside
        dataset (str): the project where the dataset resides
        table (str): the table to look for

    Returns:
        bool: True if table is present, False if not
    """
    client = bigquery.Client()
    try:
      client.get_table(f'{project}.{dataset}.{table}')
      return True

    except NotFound:
      return False

  def execute_and_wait(self, query: str) -> table.RowIterator:
    """Execute the sql in the 'query' and wait for the result.

    Commonly needed helper function

    Args:
        query (str): the query to run

    Returns:
        RowIterator: the result set
    """
    client = bigquery.Client()
    job = client.query(query)
    result = job.result()
    return result

  def run(self, context=None,
          **attributes: Mapping[str, str]) -> Dict[str, Any]:
    """Run the user's PostProcessor code

    Args:
        context ([type], optional): Cloud Function context. Defaults to None.
        **attributes: list of attributes passed to the postprocessor. These are:
            * project - the project id
            * dataset - the dataset containing the imported table
            * table - the imported table
            * report_id - the report that created the import file
            * product - one of dv360, cm, adh, ga360, sa360, sa360_report
            * rows - number of rows imported
            * columns - names of the columns in the table, ';' separated

    Returns:
        Dict[str, Any]: return value
    """
    pass
