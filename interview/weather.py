from typing import Any, Iterable, Generator
from .utils import MessageType, ControlProcessType, WeatherStreaming


def process_events(events: Iterable[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    weather_streaming = WeatherStreaming()
    for line in events:
        yield line

        if not isinstance(line, dict):
            raise ValueError("Invalid message format. Please verify input.")
        
        if "type" not in line:
            raise ValueError("No 'type' field. Please verify input.")

        message_type = line.get("type")
        if message_type == MessageType.SAMPLE:
            weather_streaming.process_sample(line)

        elif message_type == MessageType.CONTROL:
            # command = line.get("command", None)
            weather_streaming.process_control(line)

        else:
            raise ValueError(f"Unknown message type: {message_type}.\nMessage line: {line}")

