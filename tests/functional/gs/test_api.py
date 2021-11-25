"""Test the api."""
import pytest
from google.cloud import exceptions as google_exceptions

import tentaclio_gs

from . import conftest


@pytest.mark.skipif(
    not conftest.GS_TEST_URL, reason="Not used in CI as authenticated environment is needed."
)
def test_authenticated_api_calls(gs_url, bucket_exists):
    """Test the authenticated API calls.

    Skipped unless configured in conftest or TENTACLIO__CONN__GS_TEST is set.

    🚨🚨🚨WARNING🚨🚨🚨
    The functional test for GS will:
        - create a bucket if non is found.
            This will be created in the configured project (see gcloud docs).
        - upload and remove a file as set in the URL

    To use run the test use command:
    ```
    env TENTACLIO__CONN__GS_TEST=gs://tentaclio_gs-bucket/test.txt \
        make functional-gs
    ```

    You will need to have your environment configured, credentials and project.
    This is done with the gcloud cli tool. See docs for more information:
        https://googleapis.dev/python/google-api-core/latest/auth.html
    """
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with tentaclio_gs.open(gs_url, mode="wb") as f:
        f.write(data)

    with tentaclio_gs.open(gs_url, mode="rb") as f:
        result = f.read()

    assert result == data

    tentaclio_gs.remove(gs_url)

    with pytest.raises(google_exceptions.NotFound):
        with tentaclio_gs.open(gs_url, mode="rb") as f:
            result = f.read()


@pytest.mark.parametrize(
    "url,bucket,key",
    [
        ("gs://:@bucket/key", "bucket", "key"),
        ("gcs://:@bucket/key", "bucket", "key"),
    ],
)
def test_mocked_api_calls(mocker, url, bucket, key):
    """Test api calls reaches the mocks."""
    mocker.patch("tentaclio.clients.gs_client.GSClient._connect")
    m_remove = mocker.patch("tentaclio.clients.gs_client.GSClient._remove")
    m_put = mocker.patch("tentaclio.clients.gs_client.GSClient._put")
    m_get = mocker.patch("tentaclio.clients.gs_client.GSClient._get")
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with tentaclio_gs.open(url, mode="wb") as f:
        f.write(data)
    m_put.assert_called_once_with(mocker.ANY, bucket, key)

    with tentaclio_gs.open(url, mode="rb") as f:
        f.read()
    m_get.assert_called_once_with(mocker.ANY, bucket, key)

    tentaclio_gs.remove(url)
    m_remove.assert_called_once_with(bucket, key)

    m_get.reset_mock()
    m_get.side_effect = google_exceptions.NotFound("Not found")
    with pytest.raises(google_exceptions.NotFound):
        tentaclio_gs.open(url, mode="rb")
