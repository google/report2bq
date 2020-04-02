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
import json
import logging


class Report_List(object):

  def __init__(self):
    """
    Initialize Report List Class
    """


  def add_report(self, product, report_data):
    """
    Add report to config list
    Args:
      product: Product type of report
      report_data: Report data
    """

    # Ensure report type exists
    if product not in self.reports:

      # No reports of product type yet, INIT
      self.reports[product] = {}

    # Check if report already exists
    if report_data['id'] not in self.reports[product]:

      # Init report
      self.reports[product][report_data['id']] = {}

    # Add report data
    self.reports[product][report_data['id']] = report_data

    # Overwrite file
    with open(self.file_path, 'w') as file:
      json.dump(self.reports, file)


  def get_report(self, product, report_id):
    """
    Fetches report details from config list
    Args:
      product: Product type of report
      report_id: report id
    Returns:q
      Report details
    """

    # Cast report id as string
    report_id = str(report_id)

    # Check if report exists
    if report_id in self.reports[product]:

      # Return report
      return self.reports[product][report_id]

    # Return no result
    return {}


  def get_all_reports(self):
    """
    Fetches all stored reports from config list
    Returns:
      List of report details
    """

    # Return
    return self.reports


  def add_schema_to_report(self, product, report_id, schema):
    """
    Adds a schema to a report definition and re-writes it
    """
    # Ensure report type exists
    if product not in self.reports:

      # No reports of product type yet, INIT
      self.reports[product] = {}

    # Check if report already exists
    if str(report_id) not in self.reports[product]:
      return

    # Add report data
    self.reports[product][str(report_id)]['schema'] = schema

    # Overwrite file
    with open(self.file_path, 'w') as file:
      json.dump(self.reports, file)
