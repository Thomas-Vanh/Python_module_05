#!/usr/bin/env python3
import typing
import abc


class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        super().__init__()
        self._lst: list[typing.Any] = []
        self._idx: int = 0

    @abc.abstractmethod
    def validate(self, data: typing.Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: typing.Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._lst:
            raise IndexError("No data available in processor")
        oldest_data = self._lst.pop(0)
        idx = self._idx
        self._idx += 1
        return idx, str(oldest_data)


class NumericProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            return len(data) > 0 and all(isinstance(
                item, (int, float)) for item in data)
        return False

    def ingest(self, data: typing.Union[int,
                                        float,
                                        list[typing.Union
                                             [int, float]]]) -> None:
        if not self.validate(data):
            raise TypeError("improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._lst.append(str(item))
        else:
            self._lst.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return len(data) > 0 and all(
                isinstance(item, str) for item in data)
        return False

    def ingest(self, data: typing.Union[str, list[str]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._lst.append(item)
        else:
            self._lst.append(str(data))


class LogProcessor(DataProcessor):
    def validate(self, data: typing.Any) -> bool:
        if isinstance(data, dict):
            return len(data) > 0 and all(
                isinstance(k, str) and
                isinstance(v, str) for k, v in data.items())
        if isinstance(data, list):
            return len(data) > 0 and all(self.validate(item) for item in data)
        return False

    def ingest(self, data: typing.Union[dict[str, str],
                                        list[dict[str, str]]]) -> None:
        if not self.validate(data):
            raise TypeError("Improper log data")

        if isinstance(data, list):
            for item in data:
                self._lst.append(": ".join(item.values()))
        else:
            self._lst.append(": ".join(data.values()))


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print()
    print("Testing Numeric Processor...")
    num_proc = NumericProcessor()
    print(f"Trying to validate input '42': {num_proc.validate(42)}")
    print(f"Trying to validate input 'Hello': {num_proc.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num_proc.ingest("foo")
    except TypeError as e:
        print(f"Got exception: {e}")
    print("Processing data: [1, 2, 3, 4, 5]")
    num_proc.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = num_proc.output()
        print(f"Numeric value {rank}: {val}")
    print()

    print("Testing Text Processor...")
    text_proc = TextProcessor()
    print(f"Trying to validate input '42': {text_proc.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    text_proc.ingest(["Hello", "Nexus", "World"])
    print("Extracting 1 value...")
    rank, val = text_proc.output()
    print(f"Text value {rank}: {val}")
    print()

    print("Testing Log Processor...")
    log_proc = LogProcessor()
    print(f"Trying to validate input 'Hello': {log_proc.validate('Hello')}")
    logs = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR", "log_message": "Unauthorized access!!"},
    ]
    print(f"Processing data: {logs}")
    log_proc.ingest(logs)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = log_proc.output()
        print(f"Log entry {rank}: {val}")
