from . import weather
from .utils import WeatherStreaming, MessageType, ControlProcessType
from collections import defaultdict
import pytest
import json

# def test_replace_me():
#     assert [{}] == list(weather.process_events([{}]))

@pytest.fixture
def weather_streaming():
    return WeatherStreaming()

@pytest.fixture
def input_data():
    with open("input.jsonl", "r") as f:
        return [json.loads(line) for line in f]

@pytest.fixture
def expected_output():
    with open("expected_output.jsonl", "r") as f:
        return [json.loads(line) for line in f]

# Test basic initialization
def test_weather_streaming_init(weather_streaming):
    assert isinstance(weather_streaming.weather_data, defaultdict)
    assert weather_streaming.latest_timestamp is None

# Test process_sample functionality
def test_process_sample(weather_streaming):
    sample_message = {
        "type": "sample",
        "stationName": "BWU Station",
        "timestamp": 1672531200000,
        "temperature": 36.3
    }
    
    weather_streaming.process_sample(sample_message)
    
    assert weather_streaming.latest_timestamp == 1672531200000
    assert weather_streaming.weather_data["BWU Station"]["high"] == 36.3
    assert weather_streaming.weather_data["BWU Station"]["low"] == 36.3

# Test high/low temperature updates
def test_temperature_extremes(weather_streaming):
    messages = [
        {"type": "sample", "stationName": "BWU Station", "timestamp": 1, "temperature": 36.3},
        {"type": "sample", "stationName": "BWU Station", "timestamp": 2, "temperature": 37.0},
        {"type": "sample", "stationName": "BWU Station", "timestamp": 3, "temperature": 36.1}
    ]
    
    for msg in messages:
        weather_streaming.process_sample(msg)
    
    station_data = weather_streaming.weather_data["BWU Station"]
    assert station_data["high"] == 37.0
    assert station_data["low"] == 36.1


def test_generate_snapshot(weather_streaming, input_data):
    # first sample in input
    weather_streaming.process_sample(input_data[0])
    
    snapshot = weather_streaming.generate_snapshot()
    
    assert snapshot["type"] == ControlProcessType.SNAPSHOT
    assert snapshot["asOf"] == 1672531200000
    assert "Foster Weather Station" in snapshot["stations"]
    assert snapshot["stations"]["Foster Weather Station"]["high"] == 37.1
    assert snapshot["stations"]["Foster Weather Station"]["low"] == 37.1

def test_process_reset(weather_streaming):
    # Add some initial data
    weather_streaming.process_sample({
        "stationName": "BWU Station",
        "timestamp": 1672531200000,
        "temperature": 36.4
    })
    
    reset_response = weather_streaming.process_reset()
    
    assert reset_response["type"] == ControlProcessType.SNAPSHOT
    assert reset_response["asOf"] == 1672531200000
    assert len(weather_streaming.weather_data) == 0
    assert weather_streaming.latest_timestamp is None


def test_process_events(input_data, expected_output):

    results = list(weather.process_events(input_data))
    
    # Verify that all input messages are yielded
    assert len(results) == len(input_data)
    assert results == input_data


# Test complete workflow with sample input file
def test_complete_workflow(input_data):
    
    generated_events = list(weather.process_events(input_data))
    
    # Verify specific snapshots
    snapshots = [event for event in generated_events 
                if event.get("type") == "control" and 
                event.get("command") in ["snapshot","reset"]]
    
    assert len(snapshots) == 4