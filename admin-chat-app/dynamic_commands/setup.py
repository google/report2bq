from typing import Any, Dict, Mapping
from urllib.request import Request
from dynamic import DynamicClass
from googleapiclient import discovery


class Processor(DynamicClass):
  def run(self, **attributes: Mapping[str, str]) -> Dict[str, Any]:

    space={
        "space": {
          "spaceType": 'DIRECT_MESSAGE',
          "singleUserBotDm": True,
        }
      }

    result = attributes['service'].spaces().setup(space=space).execute()
    print(result)
    return {'text': 'Ok'}
