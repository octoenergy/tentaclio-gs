"""Test of the GS Client."""
import io

import pytest
from google.cloud import exceptions as google_exceptions

from tentaclio_gs.clients import gs_client


@pytest.mark.parametrize(
    "url,hostname,path",
    [
        ("gs://bucket/prefix", "bucket", "prefix"),
        ("gs://:@gs", None, ""),
        ("gs://public_key:private_key@gs", None, ""),
        ("gs://:@bucket", "bucket", ""),
        ("gs://:@bucket/prefix", "bucket", "prefix"),
    ],
)
def test_parsing_gs_url(mocker, url, hostname, path):
    """Test the parsing of the gs url."""
    mocker.patch("tentaclio_gs.clients.GSClient._connect")
    client = gs_client.GSClient(url)

    assert client.key_name == path
    assert client.bucket == hostname


def patch_string_from_method(method: str) -> str:
    """Get the string that indicates the method to be patched for a calling method."""
    switch_dict = {
        "get": "tentaclio_gs.clients.GSClient._get",
        "put": "tentaclio_gs.clients.GSClient._put",
        "remove": "tentaclio_gs.clients.GSClient._remove",
    }

    if method in switch_dict:
        return switch_dict[method]

    raise Exception(f"No patch string for calling method: {method}")


@pytest.mark.parametrize("method", ["get", "put", "remove"])
@pytest.mark.parametrize(
    "url,bucket,key",
    [("gs://:@gs", None, None), ("gs://:@gs", "bucket", None), ("gs://:@bucket", None, None)],
)
def test_invalid_path(mocker, method, url, bucket, key):
    """Test a method with invalid paths."""
    mocker.patch("tentaclio_gs.clients.GSClient._connect")
    patch_string = patch_string_from_method(method)
    mocked_method = mocker.patch(patch_string)
    # todo: find correct Exception
    with gs_client.GSClient(url) as client, pytest.raises(Exception):
        calling_method = getattr(client, mocked_method)
        if method == "remove":
            calling_method(bucket_name=bucket, key_name=key)
        else:
            calling_method(io.StringIO(), bucket_name=bucket, key_name=key)

    mocked_method.assert_not_called()


@pytest.mark.parametrize(
    "method,url,bucket,key",
    [
        ("get", "gs://:@bucket/not_found", "bucket", "not_found"),
        ("put", "gs://:@bucket/not_found", "bucket", "not_found"),
        ("remove", "gs://:@bucket/not_found", "bucket", "not_found"),
    ],
)
def test_not_found(mocker, method, url, bucket, key):
    """That when the connection raises a NotFound it is raised."""
    mocker.patch("tentaclio_gs.clients.GSClient._connect")
    patch_string = patch_string_from_method(method)
    mocked_method = mocker.patch(patch_string)
    mocked_method.side_effect = google_exceptions.NotFound("not found")
    stream = io.StringIO()
    with gs_client.GSClient(url) as client, pytest.raises(google_exceptions.NotFound):
        calling_method = getattr(client, method)
        if method == "remove":
            calling_method(bucket_name=bucket, key_name=key)
        else:
            calling_method(stream, bucket_name=bucket, key_name=key)

    if method == "remove":
        mocked_method.assert_called_once_with(bucket, key)
    else:
        mocked_method.assert_called_once_with(stream, bucket, key)


@pytest.mark.parametrize(
    "method,url,bucket,key",
    [
        ("get", "gs://bucket/prefix", "bucket", "prefix"),
        ("put", "gs://bucket/prefix", "bucket", "prefix"),
        ("remove", "gs://bucket/prefix", "bucket", "prefix"),
    ],
)
def test_method(mocker, method, url, bucket, key):
    """Test method with valid call."""
    mocker.patch("tentaclio_gs.clients.GSClient._connect")
    patch_string = patch_string_from_method(method)
    mocked_method = mocker.patch(patch_string)
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        calling_method = getattr(client, method)
        if method == "remove":
            calling_method(bucket_name=bucket, key_name=key)
        else:
            calling_method(stream, bucket_name=bucket, key_name=key)

    if method == "remove":
        mocked_method.assert_called_once_with(bucket, key)
    else:
        mocked_method.assert_called_once_with(stream, bucket, key)


@pytest.mark.parametrize(
    "method,url,bucket,key",
    [
        ("get", "gs://bucket/prefix", "bucket", "prefix"),
        ("put", "gs://bucket/prefix", "bucket", "prefix"),
        ("remove", "gs://bucket/prefix", "bucket", "prefix"),
    ],
)
def test_method_url_only(mocker, method, url, bucket, key):
    """Test method with valid call with url only."""
    mocker.patch("tentaclio_gs.clients.GSClient._connect")
    patch_string = patch_string_from_method(method)
    mocked_method = mocker.patch(patch_string)
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        calling_method = getattr(client, method)
        if method == "remove":
            calling_method()
        else:
            calling_method(stream)

    if method == "remove":
        mocked_method.assert_called_once_with(bucket, key)
    else:
        mocked_method.assert_called_once_with(stream, bucket, key)


@pytest.mark.parametrize(
    "url,bucket,key",
    [
        ("gs://bucket/prefix", "bucket", "prefix"),
    ],
)
def test_helper_get(mocker, url, bucket, key):
    """Test helper _get is correctly called."""
    mocked_connection = mocker.patch("tentaclio_gs.clients.GSClient._connect")
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        client._get(stream, bucket_name=bucket, key_name=key)
    connection = mocked_connection.return_value
    connection.bucket.assert_called_once_with(bucket)
    connection.bucket.return_value.blob.assert_called_once_with(key)
    connection.bucket.return_value.blob.return_value.download_to_file.assert_called_once()


@pytest.mark.parametrize(
    "url,bucket,key",
    [
        ("gs://bucket/prefix", "bucket", "prefix"),
    ],
)
def test_helper_put(mocker, url, bucket, key):
    """Test helper _put is correctly called."""
    mocked_connection = mocker.patch("tentaclio_gs.clients.GSClient._connect")
    stream = io.StringIO()
    with gs_client.GSClient(url) as client:
        client._put(stream, bucket_name=bucket, key_name=key)

    connection = mocked_connection.return_value
    connection.bucket.assert_called_once_with(bucket)
    connection.bucket.return_value.blob.assert_called_once_with(key)
    connection.bucket.return_value.blob.return_value.upload_from_file.assert_called_once()


@pytest.mark.parametrize(
    "url,bucket,key",
    [
        ("gs://bucket/prefix", "bucket", "prefix"),
    ],
)
def test_helper_remove(mocker, url, bucket, key):
    """Test helper _remove is correctly called."""
    mocked_connection = mocker.patch("tentaclio_gs.clients.GSClient._connect")
    with gs_client.GSClient(url) as client:
        client._remove(bucket_name=bucket, key_name=key)

    connection = mocked_connection.return_value
    connection.bucket.assert_called_once_with(bucket)
    connection.bucket.return_value.blob.assert_called_once_with(key)
    connection.bucket.return_value.blob.return_value.delete.assert_called_once()
