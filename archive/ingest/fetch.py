# Library imports
import json 
import requests
from typing import Dict

from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError

from common.logging_utils import logger

def fetch_data(url: str, headers: Dict[str, str]) -> requests.Response:
    """Fetch data from URL with standardized error handling."""
    response = requests.get(url, headers=headers, timeout=15, stream=True)
    response.raise_for_status()
    return response


def parse_protobuf_to_bytes(proto_bytes):
    """Convert raw GTFS-RT protobuf bytes into JSON bytes.

    The GTFS-RT feed is parsed into a FeedMessage, converted to a Python dict
    using ``MessageToDict`` and finally JSON-encoded to UTF-8 bytes so storage
    and downstream processing is uniform with native JSON sources.

    Args:
        proto_bytes: Raw protobuf payload.

    Returns:
        UTF-8 encoded JSON bytes representing the feed content.

    Raises:
        DecodeError: If the protobuf bytes cannot be parsed.
        Exception: Propagates unexpected errors.
    """
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(proto_bytes)
        feed_dict = MessageToDict(feed)
        return json.dumps(feed_dict).encode('utf-8')
    
    except DecodeError as e:
        logger.error(f"Failed to decode protobuf data: {e}")
        raise DecodeError(f"Invalid protobuf data: {e}")
    
    except Exception as e:
        logger.error(f"Unexpected error parsing protobuf: {e}")
        raise


def process_response(response, response_type):
    """
    Process HTTP response data based on the expected response type.
    
    This function handles different types of transit data responses:
    - 'protobuf': GTFS-RT protobuf data (converted to JSON bytes)
    - 'json': JSON API responses (re-encoded as UTF-8 bytes)
    - 'zip': GTFS static data ZIP files (returned as-is)
    
    Args:
        response: HTTP response object with .content and .json() methods
        response_type (str): Expected response type ('protobuf', 'json', or 'zip')
        
    Returns:
        bytes: Processed response data as bytes
        
    Raises:
        ValueError: If response_type is not supported
        ImportError: If protobuf libraries are needed but not available
        DecodeError: If protobuf data cannot be parsed
    """
    if response_type == 'protobuf':
        return parse_protobuf_to_bytes(response.content)
    
    elif response_type == 'json':
        return json.dumps(response.json()).encode('utf-8')
    
    elif response_type == 'zip':
        return response.content

    else:
        raise ValueError(f"Unsupported response type: {response_type}")

def get_data(url: str, headers: Dict[str, str], response_type: str) -> bytes:
    """Fetch and process data from the configured URL.
    Args:
        url (str): Source endpoint to fetch.
        headers (Dict[str, str]): HTTP headers for the request.
        response_type (str): Expected response type ('json', 'protobuf', 'zip').
    Returns:
        bytes: Processed data as bytes.
    """
    response = fetch_data(url, headers)
    data_bytes = process_response(response, response_type)
    return data_bytes
