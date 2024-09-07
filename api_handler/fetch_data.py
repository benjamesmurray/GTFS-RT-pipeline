import requests
from google.transit import gtfs_realtime_pb2
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List
from google.protobuf.message import DecodeError
import time
import logging

# Setup logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Define Pydantic models for GTFS RT data
class PositionModel(BaseModel):
    latitude: Optional[float]
    longitude: Optional[float]
    bearing: Optional[float]
    odometer: Optional[float] = None
    speed: Optional[float] = None

class VehicleInfoModel(BaseModel):
    id: Optional[str]
    label: Optional[str]
    license_plate: Optional[str]

class TripModel(BaseModel):
    trip_id: Optional[str]
    route_id: Optional[str]
    direction_id: Optional[int]
    start_time: Optional[str]
    start_date: Optional[str]
    schedule_relationship: Optional[int]

class VehiclePositionModel(BaseModel):
    id: str
    trip: Optional[TripModel]
    vehicle: Optional[VehicleInfoModel]
    position: Optional[PositionModel]
    current_stop_sequence: Optional[int]
    stop_id: Optional[str]
    current_status: Optional[int]
    timestamp: Optional[int]
    congestion_level: Optional[int]
    occupancy_status: Optional[int]
    occupancy_percentage: Optional[int]
    multi_carriage_details: Optional[List[dict]] = Field(default_factory=list)

class GTFSRTDataModel(BaseModel):
    vehicle: Optional[VehiclePositionModel]
    trip_update: Optional[dict]
    alert: Optional[dict]

def fetch_gtfs_rt_data(url: str, retries: int = 3):
    for attempt in range(retries):
        try:
            # Step 1: Fetch data from the API
            logger.info(f"Fetching data from {url}")
            response = requests.get(url, timeout=10)
            
            # Check for correct content type
            if response.headers.get('Content-Type') != 'application/x-protobuf':
                raise ValueError(f"Unexpected content type: {response.headers.get('Content-Type')}")

            # Check for empty response content
            if not response.content:
                raise Exception("No valid GTFS RT data found")

            logger.info(f"HTTP response status: {response.status_code}")

            # Step 2: Parse the Protobuf data
            feed = gtfs_realtime_pb2.FeedMessage()
            try:
                feed.ParseFromString(response.content)
                logger.info("Protobuf data parsed successfully.")
            except DecodeError as e:
                raise DecodeError(f"Error parsing message: {e}")

            # Step 3: Extract data and validate
            result = {'vehicle': None, 'trip_update': None, 'alert': None} 

            for entity in feed.entity:
                logger.info(f"Processing entity with ID: {entity.id}")

                if entity.HasField('vehicle'):
                    logger.info("Entity has 'vehicle' field.")
                    vehicle_data = {
                        'id': entity.id,
                        'trip': {field: getattr(entity.vehicle.trip, field, None) for field in ['trip_id', 'route_id', 'direction_id', 'start_time', 'start_date', 'schedule_relationship']},
                        'vehicle': {field: getattr(entity.vehicle.vehicle, field, None) for field in ['id', 'label', 'license_plate']},
                        'position': {field: getattr(entity.vehicle.position, field, None) for field in ['latitude', 'longitude', 'bearing', 'odometer', 'speed']},
                        'current_stop_sequence': getattr(entity.vehicle, 'current_stop_sequence', None),
                        'stop_id': getattr(entity.vehicle, 'stop_id', None),
                        'current_status': getattr(entity.vehicle, 'current_status', None),
                        'timestamp': getattr(feed.header, 'timestamp', None),
                        'congestion_level': getattr(entity.vehicle, 'congestion_level', None),
                        'occupancy_status': getattr(entity.vehicle, 'occupancy_status', None),
                        'occupancy_percentage': getattr(entity.vehicle, 'occupancy_percentage', None),
                        'multi_carriage_details': entity.vehicle.multi_carriage_details or []
                    }

                    try:
                        validated_data = VehiclePositionModel(**vehicle_data)
                        logger.info("Validation passed. Validated vehicle data: %s", validated_data)
                        result['vehicle'] = validated_data.model_dump()
                    except ValidationError as e:
                        logger.error("Validation error for vehicle data: %s", e.json())
                        raise e

                if entity.HasField('trip_update'):
                    result['trip_update'] = {}  # Replace with actual extraction logic

                if entity.HasField('alert'):
                    result['alert'] = {}  # Replace with actual extraction logic

            validated_result = GTFSRTDataModel(**result)
            logger.info("Validation passed for GTFS RT data model: %s", validated_result)
            return validated_result.model_dump()

        except requests.exceptions.HTTPError as e:
            # Custom exception message for HTTP errors
            logger.error(f"HTTP error: {e}")
            raise Exception(f"Error fetching GTFS RT data: {e}")

        except requests.exceptions.RequestException as e:
            # General request exception handling
            logger.error(f"Request exception occurred: {e}")
            if attempt < retries - 1:
                logger.info("Retrying... Attempt %d", attempt + 2)
                time.sleep(2)
                continue
            else:
                raise

        except DecodeError as e:
            logger.error("Protobuf decode error: %s", e)
            raise

        except ValidationError as e:
            logger.error("Validation error: %s", e)
            raise

        except ValueError as e:
            logger.error("Content type error: %s", e)
            raise

        except Exception as e:
            logger.error("General error: %s", e)
            if attempt < retries - 1:
                logger.info("Retrying... Attempt %d", attempt + 2)
                time.sleep(2)
            else:
                raise
