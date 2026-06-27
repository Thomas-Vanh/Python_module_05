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

    def ingest(self, data: typing.Union[str, list[str]]):
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
    
    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for entry in self._proc:
            proc = entry["proc"]
            collected_data: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    item = proc.output()
                    collected_data.append(item)
                except IndexError:
                    break
            if collected_data:
                plugin.process_output(collected_data)


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...

class JSONPlugin(ExportPlugin):
    def process_output(self, data):
        print("JSON output:")
        json_pairs = [f'"item_{item[0]}": "{item[1]}"' for item in data]
        json_str = "{" + ", ".join(json_pairs) + "}"
        print(json_str)


class CSVPlugin(ExportPlugin):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV output:")
        csv_row = ",".join(item[1] for item in data)
        print(csv_row)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")
    print("Initialize Data Stream...\n")
    stream_pipeline = DataStream()
    print("== DataStream statistics ==")
    stream_pipeline.print_processors_stats()

    print("\nRegistering Processors\n")
    num_p = NumericProcessor()
    text_p = TextProcessor()
    log_p = LogProcessor()
    stream_pipeline.register_processor(num_p)
    stream_pipeline.register_processor(text_p)
    stream_pipeline.register_processor(log_p)

    batch_1 = [
        'Hello world', 
        [3.14, -1, 2.71], 
        [
            {'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'}, 
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ], 
        42, 
        ['Hi', 'five']
    ]
    print(f"\nSend first batch of data on stream: {batch_1}\n")
    stream_pipeline.process_stream(batch_1)
    print("\n== DataStream statistics ==")
    stream_pipeline.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream_pipeline.output_pipeline(3, CSVPlugin())
    print("\n== DataStream statistics ==")
    stream_pipeline.print_processors_stats()

    batch_2 = [
        21, 
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'], 
        [
            {'log_level': 'ERROR', 'log_message': '500 server crash'}, 
            {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}
        ], 
        [32, 42, 64, 84, 128, 168], 
        'World hello'
    ]
    print(f"Send another batch of data: {batch_2}")
    stream_pipeline.process_stream(batch_2)
    print("\n== DataStream statistics ==")
    stream_pipeline.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream_pipeline.output_pipeline(5, JSONPlugin())
    print("\n== DataStream statistics ==")
    stream_pipeline.print_processors_stats()