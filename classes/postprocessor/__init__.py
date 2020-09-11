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
import logging
import os
import sys

from contextlib import suppress
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec
from importlib.util import spec_from_file_location
from typing import Any, Dict, Mapping

from classes.cloud_storage import Cloud_Storage


class PostProcessorFinder(MetaPathFinder):
  def find_spec(self, fullname, path, target=None):
    """
    Locate the file in GCS. The "spec" should then be 
    fullname = fullname
    path = location in GCS, should be hardwired in config
    """
    if 'postprocessor' not in fullname: 
      return None                     # we don't handle this this

    else:
      return ModuleSpec(fullname, PostProcessorLoader())


class PostProcessorLoader(Loader):
    def create_module(self, spec):
      return None # use default module creation semantics

    def exec_module(self, module):
      try:
        # Fetch the code here as string:
        # GCS? BQ? Firestore? All good options
        filename = module.__name__.split('.')[-1]
        code = Cloud_Storage.fetch_file(
          bucket=f'{os.environ.get("GCP_PROJECT")}-report2bq-postprocessor', 
          file=f'{filename}.py'
        )
        exec(code, vars(module))
      except:
        raise ModuleNotFoundError()


class PostProcessor(object):
  def run(self, context=None, **attributes: Mapping[str, str]) -> Dict[str, Any]: pass


def install_postprocessor():
    """Inserts the finder into the import machinery"""
    sys.meta_path.append(PostProcessorFinder())
