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

from enum import Enum

class Type(Enum):
  DV360 = 'dbm'
  CM = 'dcm'
  SA360 = 'sa360'
  ADH = 'adh'
  
  # def __new__(cls, value):
  #   if value == 'dv360': 
  #     obj = DV360
  #   elif value == 'cm': 
  #     obj = CM
  #   else:
  #     obj = object.__new__(cls)

  #   return obj


  def __str__(self):
    return str(self.value)