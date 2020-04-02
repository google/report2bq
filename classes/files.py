"""
Copyright 2018 Google LLC

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


class Files(object):

  @staticmethod
  def get_file_path(path_from_root):
    """
    Returns absolute file path
    Args:
      path_from_root: relative path from project root
    Returns:
      Absolute file path from project root directory
    """
    # Project root
    root = os.path.dirname(os.path.realpath(sys.argv[0]))

    # Absolute path
    abs_file_path = '{root}{path_from_root}'.format(
        root = root,
        path_from_root = path_from_root
    )

    # Return
    return abs_file_path


  @staticmethod
  def remove_local_report_file(report_id):
    os.remove(Files.get_file_path('/report_files/{report_id}.csv'.format(
        report_id = report_id
    )))


  @staticmethod
  def fetch_file(bucket, file):
    client = storage.Client()

    try:
      content = client.get_bucket(bucket).blob(file).download_as_string()
    except Exception as ex:
      logging.error('Error fetching file {f}\n{e}'.format(f=file, e=ex))

    return content
