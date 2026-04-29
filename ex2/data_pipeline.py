from typing import Any, Union, List, Dict
from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []
        self._total_processed: int = 0
        self._index: int = -1

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        self._index += 1
        return (self._index, self._storage.pop(0))


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


class ExportPlugin(typing.Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class ExportCSV:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        if (len(data) == 0):
            print("--No data to output")
            return
        print(",".join(value[1] for value in data))


class ExportJSON:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        if (len(data) == 0):
            print("--No data to output")
            return
        output = {"item " + str(value[0]): value[1] for value in data}
        print(output)


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        if proc in self._processors:
            raise ValueError("Processor already registered.")
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
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
        if not self._processors:
            print("no data processed yet")
            return
        for proc in self._processors:
            name = proc.__class__.__name__
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

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self._processors:
            to_export: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    to_export.append(proc.output())
                except IndexError:
                    break
            plugin.process_output(to_export)


def test_data_pipeline() -> None:
    print("=== Code Nexus - Data Stream ===\n")

    print("Initialize Data Stream...\n")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processor")
    stream.register_processor(NumericProcessor())
    stream.register_processor(TextProcessor())
    stream.register_processor(LogProcessor())

    batch = ["Hello world", [3.14, -1, 2.71],
             [{"log_level": "WARNING", "log_message":
               "Telnet acess! Use ssh instead"},
             {"log_level": "INFO", "log_message": "User wil is connected"}],
             42, ["Hi", "five"]]
    print(f"\nSend first batch of data on stream: {batch}\n")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    stream.output_pipeline(3, ExportCSV())
    print()
    stream.print_processors_stats()

    batch = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
             [{'log_level': 'ERROR', 'log_message': '500 server crash'},
             {'log_level': 'NOTICE', 'log_message':
              'Certificate expires in 10 days'}],
             [32, 42, 64, 84, 128, 168], 'World hello']
    print(f"\nSend another batch of data on stream: {batch}\n")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    stream.output_pipeline(5, ExportJSON())
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    test_data_pipeline()
