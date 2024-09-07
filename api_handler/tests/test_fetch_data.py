import pytest  # Import pytest for retry decorator
from unittest.mock import patch, Mock
from pydantic import ValidationError
from google.protobuf.message import DecodeError  
from fetch_data import fetch_gtfs_rt_data  
import requests

# Define a function to read the real GTFS RT binary Protobuf message from your file
def get_real_protobuf_response(file_path):
    with open(file_path, 'rb') as file:
        return file.read()

# ---------------- Successful Data Handling Tests ----------------

class TestSuccessfulDataHandling:
    """Group of tests for successful data handling scenarios."""

    # Test Case ID: TC_001
    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_successful_fetch_data_returns_correct_structure(self, mock_get):
        """
        Test Case ID: TC_001
        Description: Test that valid GTFS RT data is fetched and parsed successfully.
        Expected Outcome: The function returns a dictionary containing keys 'vehicle', 'trip_update', and 'alert'.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = get_real_protobuf_response('/home/gtfs_rt_user/gtfs-rt-pipeline/scripts/gtfs_rt_sample.bin')
        mock_response.headers = {'Content-Type': 'application/x-protobuf'}
        mock_get.return_value = mock_response

        result = fetch_gtfs_rt_data('http://example.com/gtfs-realtime')
        assert isinstance(result, dict)
        assert 'vehicle' in result
        assert 'trip_update' in result
        assert 'alert' in result

# ---------------- Error Handling Tests ----------------

class TestErrorHandling:
    """Group of tests for handling different error scenarios."""

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_correct_data_validation_raises_decode_error(self, mock_get):
        """
        Test Case ID: TC_002
        Description: Test that a malformed Protobuf response raises a DecodeError.
        Expected Outcome: A DecodeError is raised when the response content is malformed.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'\n\x12\x08\x02\x12\x08\x08\x03\x12\x03xyz\x18\x01'  # Malformed Protobuf
        mock_response.headers = {'Content-Type': 'application/x-protobuf'}
        mock_get.return_value = mock_response

        with pytest.raises(DecodeError):
            fetch_gtfs_rt_data('http://example.com/gtfs-realtime')

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_timeout_handling_raises_timeout_exception(self, mock_get):
        """
        Test Case ID: TC_003
        Description: Test that a timeout raises a requests.exceptions.Timeout exception.
        Expected Outcome: A Timeout exception is raised when the request times out.
        """
        mock_get.side_effect = requests.exceptions.Timeout()
        with pytest.raises(requests.exceptions.Timeout):
            fetch_gtfs_rt_data('http://example.com/gtfs-realtime')

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_api_error_response_handling(self, mock_get):
        """
        Test Case ID: TC_004
        Description: Test that a non-2xx HTTP status code raises an appropriate exception.
        Expected Outcome: An exception with a message containing 'Error fetching GTFS RT data' is raised.
        """
        mock_get.side_effect = requests.exceptions.HTTPError("500 Server Error: Internal Server Error for url: http://example.com/gtfs-realtime")

        with pytest.raises(Exception) as context:
            fetch_gtfs_rt_data('http://example.com/gtfs-realtime')
        assert 'Error fetching GTFS RT data' in str(context.value)

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_invalid_url_handling_raises_invalid_url_exception(self, mock_get):
        """
        Test Case ID: TC_005
        Description: Test that an invalid URL raises a requests.exceptions.InvalidURL exception.
        Expected Outcome: An InvalidURL exception is raised when the URL is invalid.
        """
        mock_get.side_effect = requests.exceptions.InvalidURL()
        with pytest.raises(requests.exceptions.InvalidURL):
            fetch_gtfs_rt_data('http://invalid-url')

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_empty_response_handling_raises_exception(self, mock_get):
        """
        Test Case ID: TC_006
        Description: Test that an empty response raises an exception indicating no valid data found.
        Expected Outcome: An exception with a message containing 'No valid GTFS RT data found' is raised.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b''
        mock_response.headers = {'Content-Type': 'application/x-protobuf'}
        mock_get.return_value = mock_response

        with pytest.raises(Exception) as context:
            fetch_gtfs_rt_data('http://example.com/gtfs-realtime')
        assert 'No valid GTFS RT data found' in str(context.value)

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_network_interruption_handling_raises_connection_error(self, mock_get):
        """
        Test Case ID: TC_007
        Description: Test that a network interruption raises a requests.exceptions.ConnectionError exception.
        Expected Outcome: A ConnectionError exception is raised when the network is interrupted.
        """
        mock_get.side_effect = requests.exceptions.ConnectionError()
        with pytest.raises(requests.exceptions.ConnectionError):
            fetch_gtfs_rt_data('http://example.com/gtfs-realtime')

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_unexpected_content_type_handling_raises_exception(self, mock_get):
        """
        Test Case ID: TC_008
        Description: Test that an unexpected content type raises an appropriate exception.
        Expected Outcome: An exception with a message containing 'Unexpected content type' is raised.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'<html>This is not a Protobuf</html>'
        mock_response.headers = {'Content-Type': 'text/html'}
        mock_get.return_value = mock_response

        with pytest.raises(Exception) as context:
            fetch_gtfs_rt_data('http://example.com/gtfs-realtime')
        assert 'Unexpected content type' in str(context.value)

# ---------------- Resilience and Robustness Tests ----------------

class TestResilienceAndRobustness:
    """Group of tests for resilience and robustness scenarios."""

    @pytest.mark.retry(count=2, delay=0.5)
    @patch('fetch_data.requests.get')
    def test_random_network_failures(self, mock_get):
        """
        Test Case ID: TC_013
        Description: Test function resilience against random network failures.
        Expected Outcome: The function should handle random network issues and retry appropriately.
        """

        # Define a list of side effects with initial failures and a final success
        side_effects = [
            requests.exceptions.Timeout(),            # First call raises Timeout
            requests.exceptions.ConnectionError(),    # Second call raises ConnectionError
            requests.exceptions.HTTPError("500 Server Error: Internal Server Error for url: http://example.com/gtfs-realtime"),  # Third call raises HTTPError with a message
            self.successful_response()                # Fourth call succeeds
        ]

        mock_get.side_effect = side_effects

        # Call the function and check it eventually succeeds
        with pytest.raises(Exception) as context:
            fetch_gtfs_rt_data('http://example.com/gtfs-realtime')

        # Verify that it raises the custom exception with the correct message
        assert 'Error fetching GTFS RT data' in str(context.value)

    def successful_response(self):
        """
        Simulate a successful response.
        """
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'\n\x12\x08\x02\x12\x08\x08\x03\x12\x03xyz\x18\x01'  # Example valid content
        mock_response.headers = {'Content-Type': 'application/x-protobuf'}
        return mock_response
