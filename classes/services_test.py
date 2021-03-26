import unittest

from classes import services

SA360_DEFINITION = \
  services.ServiceDefinition(name='doubleclicksearch', version='v2')
GMAIL_ARGS = {
  'serviceName': 'gmail',
  'version': 'v1',
}

class ServicesTest(unittest.TestCase):
  def test_valid_service(self):
    self.assertGreater(services.Service.SA360.value, 0)

  def test_single_definition(self):
    self.assertEqual(SA360_DEFINITION, services.Service.SA360.definition)

  def test_single_to_args(self):
    self.assertEqual(GMAIL_ARGS, services.Service.GMAIL.definition.to_args)

  def test_all_definitions(self):
    for id in range(1, 7):
      S = services.Service(id)
      self.assertEqual(services.SERVICE_DEFINITIONS[S], S.definition)


if __name__ == '__main__':
  unittest.main()
