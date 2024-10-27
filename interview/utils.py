import json
from collections import defaultdict
from typing import Iterable, Generator, Any, Dict
from enum import StrEnum

class MessageType(StrEnum):
    SAMPLE = "sample"
    CONTROL = "control"

class ControlProcessType(StrEnum):
    SNAPSHOT = "snapshot"
    RESET = "reset"


class WeatherStreaming:
    def __init__(self):
        self.weather_data = defaultdict(lambda: {"high": float("-inf"), "low": float("inf")})
        self.latest_timestamp = None

    def process_sample(self, message: Dict[str, Any]):
        station_name = message.get("stationName")
        temperature = message.get("temperature")
        timestamp = message.get("timestamp")

        if self.latest_timestamp is None or timestamp > self.latest_timestamp:
            self.latest_timestamp = timestamp

        # Update high/low temperatures for the station
        station_data = self.weather_data[station_name]
        station_data["high"] = max(station_data["high"], temperature)
        station_data["low"] = min(station_data["low"], temperature)
        station_data["timestamp"] = timestamp #FIXME: rely on lastest timestamp?

    def process_control(self, message: Dict[str, Any]) -> Dict[str, Any]:
        
        try:
            command = message.get("command")
            if command == ControlProcessType.SNAPSHOT:
                return self.generate_snapshot()
            
            elif command == ControlProcessType.RESET:
                return self.process_reset()
        
        except ValueError as e:
            if "Please verify input" in str(e):
                raise ValueError(f"The program is generated using a large language model. Please verify input.\nOriginal error: {e}")
            else:
                raise ValueError(e)
            
        return {}
        

    def generate_snapshot(self) -> Dict[str, Any]:

        stations = {
            name: {"high": data["high"], "low": data["low"]}
            for name, data in self.weather_data.items()
            if data["timestamp"]<=self.latest_timestamp #FIXME: rely on lastest timestamp?
        }

        return {
            "type": ControlProcessType.SNAPSHOT,
            "asOf": self.latest_timestamp,
            "stations": stations
        }

    def process_reset(self) -> Dict[str, Any]:

        self.weather_data.clear()
        reset_time = self.latest_timestamp
        self.latest_timestamp = None

        return {
            "type": ControlProcessType.SNAPSHOT,
            "asOf": reset_time
        }

