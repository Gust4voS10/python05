from typing import Any, Union, List, Dict
from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._total_processed: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> str:
        if not self._storage:
            raise IndexError("No data available in the processor.")
        return self._storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if (
            isinstance(data, list) and
            all(isinstance(i, (int, float))for i in data)
        ):
            return True
        return False

    def ingest(self, data: Union[int, float, List[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._total_processed += 1
                self._storage.append(str(item))
        else:
            self._total_processed += 1
            self._storage.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list) and all(isinstance(i, str) for i in data):
            return True
        return False

    def ingest(self, data: Union[str, List[str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._total_processed += 1
                self._storage.append(item)
        else:
            self._total_processed += 1
            self._storage.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_log_dict(d: dict[str, str]) -> bool:
            return (
                isinstance(d, dict)
                and all(isinstance(k, str) and
                        isinstance(v, str) for k, v in d.items())
            )

        if is_log_dict(data):
            return True
        if isinstance(data, list) and all(is_log_dict(i) for i in data):
            return True
        return False

    def ingest(self, data: Union[Dict[str, str],
                                 List[Dict[str, str]]]) -> None:
        if not self.validate(data):
            raise ValueError(f"Dados inválidos para LogProcessor: {data}")

        if isinstance(data, list):
            for d in data:
                formatted = f"{d['log_level'].strip()}: {d['log_message']}"
                self._storage.append(formatted)
                self._total_processed += 1
        else:
            formatted = f"{data['log_level'].strip()}: {data['log_message']}"
            self._storage.append(formatted)
            self._total_processed += 1


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        if proc in self._processors:
            raise ValueError("Processor already registered.")
        self._processors.append(proc)

    def process_stream(self, stream: list[typing.Any]) -> None:
        for item in stream:
            accepted = False
            for proc in self._processors:
                if proc.validate(item):
                    proc.ingest(item)
                    accepted = True
                    break
            if not accepted:
                print(f"DataStream error - Can't process "
                      f"element in stream: {item}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        for proc in self._processors:
            name = proc.__class__.__name__
            if len(proc._storage) == 0:
                print("no data processed yet")
            if name == "NumericProcessor":
                print(f"Numeric Processor: total {proc._total_processed} items"
                      f" processed, remaining "
                      f"{len(proc._storage)} on processor")
            elif name == "TextProcessor":
                print(f"Text Processor: total {proc._total_processed} items "
                      f"processed, remaining "
                      f"{len(proc._storage)} on processor")
            elif name == "LogProcessor":
                print(f"Log Processor: total {proc._total_processed} items "
                      f"processed, remaining "
                      f"{len(proc._storage)} on processor")


def test_data_stream() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    stream.register_processor(NumericProcessor())

    batch = ["Hello world", [3.14, -1, 2.71],
             [{"log_level": "WARNING", "log_message":
               "Telnet acess! Use ssh instead"},
             {"log_level": "INFO", "log_message": "User wil is connected"}],
             42, ["Hi", "five"]]
    print(f"\nSend first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other text processors\nSend the same batch again")
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    i = 3
    for processor in stream._processors:
        for i in range(i):
            processor.output()
    stream.print_processors_stats()


if __name__ == "__main__":
    test_data_stream()
