from enum import Enum
from typing import Any, Dict


class Service(Enum):
  SCHEDULER = 'scheduler'
  
  def __str__(self):
    return str(self.value)


  def definition(self) -> Dict[str, Any]:
    defs = {
      'scheduler': {
        'serviceName': 'cloudscheduler',
        'version': 'v1',
      },
    }
    return defs.get(self.value, {})

