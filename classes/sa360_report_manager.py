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

import json
import logging
import pprint

# Class Imports
from contextlib import suppress
from datetime import datetime
from urllib.parse import unquote

from classes.firestore import Firestore
from classes.report_type import Type
from classes.scheduler import Scheduler


class SA360Manager(object):
  def manage(self, **kwargs):
    firestore = Firestore(project=kwargs['project'], email=kwargs['email'])

    args = {
      'report': kwargs.get('name', kwargs.get('file').split('/')[-1].split('.')[0] if kwargs.get('file') else None),
      'file': kwargs.get('file'),
      'firestore': firestore,
      'project': kwargs['project'],
      'email': kwargs['email'],
      **kwargs,
    }

    action = {
      'list': self.list_all,
      'show': self.show,
      'add': self.add,
      'delete': self.delete,
    }.get(kwargs['action'])
    
    if action:
      return action(**args)

    else:
      raise NotImplementedError()


  def list_all(self, firestore: Firestore, project: str, _print: bool=False, **unused): 
    reports = firestore.list_documents(Type.SA360_RPT, '_reports')
    if _print:
      print(f'SA360 Dynamic Reports defined for project {project}')
      print()
      for report in reports:
        print(f'  {report}')

    return reports

  def show(self, firestore: Firestore, report: str, _print: bool=False, **unused):
    definition = firestore.get_document(Type.SA360_RPT, '_reports').get(report)
    if _print:
      print(f'SA360 Dynamic Report "{report}"')
      print()
      pprint.pprint(definition, indent=2, compact=False)

    return definition


  def add(self, firestore: Firestore, report: str, file: str, **unused): 
    with open(file) as definition:
      cfg = json.loads(''.join(definition.readlines()))
      Firestore().update_document(Type.SA360_RPT, '_reports', { report: cfg })


  def delete(self, firestore: Firestore, project: str, report: str, email: str, **unused): 
    firestore.delete_document(Type.SA360_RPT, '_reports', report)
    scheduler = Scheduler()
    args = {
      'action': 'list',
      'email': email,
      'project': project,
      'html': False,
    }

    # Disable all runners for the now deleted report
    runners = list(runner['name'].split('/')[-1] for runner in scheduler.process(args) if report in runner['name'])
    for runner in runners:
      args = {
        'action': 'disable',
        'email': None,
        'project': project,
        'job_id': runner,
      }
      scheduler.process(args)
