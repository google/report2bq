from enum import Enum
from typing import Any, Dict


class Service(Enum):
  SCHEDULER = 'scheduler'
  DV360 = 'dv360'
  CM = 'cm'
  
  def __str__(self):
    return str(self.value)


  def definition(self) -> Dict[str, Any]:
    defs = {
      'scheduler': {
        'serviceName': 'cloudscheduler',
        'version': 'v1',
      },
      'cm': {
        'serviceName': 'dfareporting',
        'version': 'v3.3',
      },
      'dv360': {
        'serviceName': 'doubleclickbidmanager',
        'version': 'v1.1'
      },
    }
    return defs.get(self.value, {})

