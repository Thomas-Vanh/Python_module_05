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


class DataStream():
    def __init__(self) -> None:
        self._proc: list[dict[str, typing.Any]] = []

    def register_processor(self, proc:  DataProcessor) -> None:
        self._proc.append({
            "proc": proc,
            "total": 0
        })

    def process_stream(self, stream: list[typing.Any]) -> None:
        for element in stream:
            handled: bool = False
            for entry in self._proc:
                if entry["proc"].validate(element):
                    entry["proc"].ingest(element)
                    entry["total"] += 1
                    handled = True
                    break
            if not handled:
                print("DataStream error - Can't process element in stream:"
                      f"{element}")

    def print_processors_stats(self) -> None:
        if not self._proc:
            print("No processor found, no data")
            return
        for entry in self._proc:
            proc = entry["proc"]
            name: str = proc.__class__.__name__
            rem_proc: int = len(proc._lst)
            print(f"{name}: total {len(proc._lst)} items processed,"
                  f" remaining {rem_proc} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    print("Initialize Data Stream...")

    stream: DataStream = DataStream()
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print("\nRegistering Numeric Processor\n")
    num_proc: NumericProcessor = NumericProcessor()
    stream.register_processor(num_proc)
    batch: list[typing.Any] = ['Hello world',
                               [3.14, -1, 2.71],
                               [{'log_level': 'WARNING',
                                 'log_message':
                                 'Telnet access! Use ssh instead'},
                                {
                                  'log_level': 'INFO',
                                  'log_message': 'User wil is connected'}],
                               42,
                               ['Hi', 'five']]
    print(f"Send first batch of data stream: {batch}")
    stream.process_stream(batch)
    print("== DataStream statistics ==")
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    txt_proc: TextProcessor = TextProcessor()
    log_proc: LogProcessor = LogProcessor()
    stream.register_processor(txt_proc)
    stream.register_processor(log_proc)
    print("Send the same batch again")
    stream.process_stream(batch)
    print("== DataStream statistics ==")
    stream.print_processors_stats()
    print("\nConsume some elements from the data processors:"
          "Numeric 3, Text 2, Log 1")
    for i in range(3):
        num_proc.output()
    for i in range(2):
        txt_proc.output()
    log_proc.output()
    print("== DataStream statistics ==")
    stream.print_processors_stats()
