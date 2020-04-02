from enum import Enum

class Type(Enum):
  DBM = 'dbm'
  DV360 = 'dbm'
  DCM = 'dcm'
  CM = 'dcm'
  SA360 = 'sa360'
  ADH = 'adh'
  

  def __str__(self):
    return str(self.value)