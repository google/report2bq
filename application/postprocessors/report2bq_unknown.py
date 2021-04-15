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

import logging

from typing import Any, Dict, Mapping

from classes.postprocessor import PostProcessor


class Processor(PostProcessor):
  def __init__(self) -> None:
    logging.info('Unknown Post-Processor')

  def run(self, context=None, **attributes: Mapping[str, str]) -> Dict[str, Any]:
    if context:
      logging.error(
        (f'Error Post-Processor triggered by messageId {context.event_id} '
         f'published at {context.timestamp} '
         'with attributes\n'
         f'{attributes}')
      )
