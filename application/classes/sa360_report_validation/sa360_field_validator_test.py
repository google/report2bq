import unittest
from unittest import mock

from classes.sa360_report_validation.campaign import Campaign
from googleapiclient import discovery


SA360_SAVED_COLUMNS = {
      "kind": "doubleclicksearch#savedColumnList",
      "items": [
        {
          "kind": "doubleclicksearch#savedColumn",
          "savedColumnName": "Foo",
          "type": "string"
        },
        {
          "kind": "doubleclicksearch#savedColumn",
          "savedColumnName": "Bar",
          "type": "string"
        },
      ]
    }
class SA360FieldValidatorTest(unittest.TestCase):

  def setUp(self):
    self.campaign = Campaign()

  def test_valid_standard_column(self):
    self.assertEqual(
      (True, 'adWordsConversions'),
      self.campaign.validate({'value': 'adWordsConversions', 'type': 'columnName'}))

  def test_valid_missing_standard_column(self):
    self.assertEqual(
      (True, '--- Blank column name ---'),
      self.campaign.validate({'value': '', 'type': 'columnName'}))

  def test_valid_missing_custom_column(self):
    self.assertEqual(
      (True, '--- Blank column name ---'),
      self.campaign.validate({'value': '', 'type': 'savedColumnName'}))

  def test_invalid_standard_column_case(self):
    self.assertEqual(
      (False, 'adWordsConversions'),
      self.campaign.validate({'value': 'adWordsConversionS', 'type': 'columnName'}))

  def test_invalid_standard_column(self):
    self.assertEqual(
      (False, None),
      self.campaign.validate({'value': 'foo', 'type': 'columnName'}))

  def test_invalid_savedColumnName(self):
    self.assertEqual(
      (False, '--- No custom columns found ---'),
      self.campaign.validate({'value': 'foo', 'type': 'savedColumnName'}))

  @mock.patch.object(
      discovery,
      'Resource')
  def test_invalid_column_definition(self, mock_sa360_service):
    mock_sa360_service.savedColumns().list().execute.return_value = {}
    campaign = Campaign(sa360_service=mock_sa360_service,
                        agency='agency',
                        advertiser='advertiser')
    self.assertEqual(
      (False, '--- No custom columns found ---'),
      campaign.validate('foo'))

  @mock.patch.object(
      discovery,
      'Resource')
  def test_did_you_mean_bad_case(self, mock_sa360_service):
    mock_sa360_service.savedColumns().list().execute.return_value = SA360_SAVED_COLUMNS
    campaign = Campaign(sa360_service=mock_sa360_service,
                        agency='agency',
                        advertiser='advertiser')
    self.assertEqual(
      (False, 'Foo'),
      campaign.validate({'value': 'foo', 'type': 'savedColumnName'}))

  @mock.patch.object(
      discovery,
      'Resource')
  def test_did_you_mean_bad_name(self, mock_sa360_service):
    mock_sa360_service.savedColumns().list().execute.return_value = SA360_SAVED_COLUMNS
    campaign = Campaign(sa360_service=mock_sa360_service)
    self.assertEqual(
      (False, None),
      campaign.validate({'value': 'baz', 'type': 'savedColumnName'}))

if __name__ == '__main__':
  unittest.main()
