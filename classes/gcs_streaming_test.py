"""Tests for google3.third_party.gps_building_blocks.py.cloud.utils.cloud_storage_streaming."""
import os
import queue

from google.auth import credentials
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage

import unittest
from unittest import mock

from google.resumable_media.requests.upload import ResumableUpload
from classes import gcs_streaming as cloud_storage_streaming

TEST_BYTES = b'01234567890123456789012345678901234'


class CloudStorageStreamingTest(unittest.TestCase):

  def setUp(self):
    super(CloudStorageStreamingTest, self).setUp()
    self.mock_bucket_name = 'bucket'
    self.mock_blob_name = 'test.csv'

    self.mock_credentials = mock.create_autospec(credentials.Credentials)
    self.mock_client = \
      mock.create_autospec(storage.Client, _credentials=self.mock_credentials)
    self.mock_authorized_session = mock.create_autospec(AuthorizedSession)
    self.mock_resumable_upload = mock.create_autospec(ResumableUpload)

    self.mock_bucket = \
      mock.create_autospec(storage.Bucket, name=self.mock_bucket_name)
    self.mock_blob = \
      mock.create_autospec(storage.Blob, name=self.mock_blob_name)
    self.mock_bucket.blob.return_value = self.mock_blob
    self.mock_bucket.get_blob.return_value = self.mock_blob
    self.streamer_queue = queue.Queue()

    with mock.patch(
        'google.resumable_media.requests.ResumableUpload',
        return_value=self.mock_resumable_upload):
      with mock.patch(
        'google.auth.transport.requests.AuthorizedSession',
        return_value=self.mock_authorized_session):
        self._threaded_test = \
          cloud_storage_streaming.ThreadedGCSObjectStreamUpload(
            client=self.mock_client,
            bucket_name=self.mock_bucket_name,
            blob_name=self.mock_blob_name,
            streamer_queue=self.streamer_queue,
            chunk_size=10
          )


  def test_write_threaded(self):
    streamer = self._threaded_test
    # streamer.start()

    # self.streamer_queue.put(TEST_BYTES)
    # self.streamer_queue.join()
    # streamer.stop()


if __name__ == '__main__':
  unittest.main()
