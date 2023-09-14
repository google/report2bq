from typing import Any, Dict, Mapping
from dynamic import DynamicClass


class Processor(DynamicClass):
  def run(self, **attributes: Mapping[str, str]) -> Dict[str, Any]:
    return {'text': 'Hello world!'}

