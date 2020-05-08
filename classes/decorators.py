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

from functools import wraps


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


def retry(exceptions, tries: int=4, delay: int=3, backoff: int=2):
  """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """

  def deco_retry(f):

    @wraps(f)
    def f_retry(*args, **kwargs):
      mtries, mdelay = tries, delay
      while mtries > 1:
        try:
          return f(*args, **kwargs)
        except exceptions as e:
          msg = "{}, Retrying in {} seconds...".format(e, mdelay)
          logging.warning(msg)
          time.sleep(mdelay)
          mtries -= 1
          mdelay *= backoff
      return f(*args, **kwargs)

    return f_retry  # true decorator

  return deco_retry
