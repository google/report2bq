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

# Python Imports
import os
import logging
import sys
from google.cloud import storage


def get_file_path(path_from_root):
  """
  Returns absolute file path
  Args:
    path_from_root: relative path from project root
  Returns:
    Absolute file path from project root directory
  """
  root = os.path.dirname(os.path.realpath(sys.argv[0]))
  return f'{root}{path_from_root}'
