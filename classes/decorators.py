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
import time
import tracemalloc


def timeit(method):
  def timed(*args, **kw):
    ts = time.time()
    try:
      return method(*args, **kw)
    finally:
      te = time.time()
      logging.info(f'{method.__name__} {(te - ts) * 1000:0.3f}ms')
      # return result
  return timed


def measure_memory(method):
  def decorate(*args, **kw):
    try:
      tracemalloc.start()
      ts = time.time()
      return method(*args, **kw)
    finally:
      te = time.time()
      current, peak = tracemalloc.get_traced_memory()
      logging.info(f'Function Name        : {method.__name__}')
      logging.info(f'Execution time       : {(te - ts) * 1000:0.3f}ms')
      logging.info(f'Current memory usage : {current / 10**6:>04.3f}M')
      logging.info(f'Peak                 : {peak / 10**6:>04.3f}M')
      tracemalloc.stop()
  return decorate