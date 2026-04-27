from typing import Any, Union, List, Dict
from abc import ABC, abstractmethod
import typing


class DataProcessor(ABC):
    def __init__(self):
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
            self._total_processed += 1
            return True
        if (
            isinstance(data, list) and
            all(isinstance(i, (int, float))for i in data)
        ):
            self._total_processed += len(data)
            return True
        return False

    def ingest(self, data: Union[int, float, List[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(str(item))
        else:
            self._storage.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            self._total_processed += 1
            return True
        if isinstance(data, list) and all(isinstance(i, str) for i in data):
            self._total_processed += len(data)
            return True
        return False

    def ingest(self, data: Union[str, List[str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        if isinstance(data, list):
            for item in data:
                self._storage.append(item)
        else:
            self._storage.append(data)


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_log_dict(d):
            return (
                isinstance(d, dict)
                and all(isinstance(k, str) and
                        isinstance(v, str) for k, v in d.items())
            )

        if is_log_dict(data):
            self._total_processed += 1
            return True
        if isinstance(data, list) and all(is_log_dict(i) for i in data):
            self._total_processed += len(data)
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
        else:
            formatted = f"{data['log_level'].strip()}: {data['log_message']}"
            self._storage.append(formatted)

    class DataStream:
        def __init__(self):
            self._processors: list[DataProcessor] = []

        def register_processor(self, proc: DataProcessor) -> None:
            if proc in self._processors:
                raise ValueError("Processor already registered.")
            self._processors.append(proc)

        def process_stream(self, stream: list[typing.Any]) -> None:
            for item in stream:
                for proc in self._processors:
                    if proc.validate(item):
                        proc.ingest(item)
                        break

        def print_processors_stats(self) -> None:
            print("== DataStream statistics ==")
            found = False
            for proc in self._processors:
                name = proc.__class__.__name__
                if proc._storage is None:
                    found = True
                if name == "NumericProcessor":
                    print(f"Numeric Processor: total {} items "
                          f"processed, remaining {} on processor")
                elif name == "TextProcessor":
                    print(f"Text Processor: total {})} items "
                          f"processed, remaining {})} on processor")
                elif name == "LogProcessor":
                    print(f"Log Processor: total {} items "
                          f"processed, remaining {} on processor")
            if not found:
                print("No processor found, no data")


def test():
    print("Testing Numeric Processor...")
    np = NumericProcessor()

    print(f"Trying to validate input '42': {np.validate(42)}")
    print(f"Trying to validate input 'Hello': {np.validate('Hello')}")

    try:
        print("Test invalid ingestion of string "
              "'foo' without prior validation:")
        np.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")

    data = [1, 2, 3, 4, 5]
    print(f"Processing data: {data}")
    np.ingest(data)

    print("Extracting 3 values...")
    for i in range(3):
        print(f"Numeric value {i}: {np.output()}")

    print("\nTesting Text Processor...")
    tp = TextProcessor()

    print(f"Trying to validate input '42': {tp.validate(42)}")

    text_data = ["Hello", "Nexus", "World"]
    print(f"Processing data: {text_data}")
    tp.ingest(text_data)

    print("Extracting 1 value...")
    print(f"Text value 0: {tp.output()}")

    print("Testing Log Processor...")
    lp = LogProcessor()

    print(f"Trying to validate input 'Hello': {lp.validate('Hello')}")

    log_data = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR\n", "log_message": "Unauthorized access!!"},
    ]

    print(f"Processing data: {log_data}")
    lp.ingest(log_data)

    print("Extracting 2 values...")
    for i in range(2):
        print(f"Log entry {i}: {lp.output()}")


def test_data_stream():
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")
    ds = LogProcessor.DataStream()
    print("Registering Numeric Processor")
    num_proc = NumericProcessor()
    ds.register_processor(num_proc)
    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [
            {'log_level': 'WARNING',
             'log_message': 'Telnet access! Use ssh instead'},
            {'log_level': 'INFO', 'log_message': 'User wil is connected'}
        ],
        42,
        ['Hi', 'five']
    ]
    print(f"Send first batch of data on stream: {batch}")
    for item in batch:
        try:
            ds.process_stream([item])
        except Exception:
            pass
        if not num_proc.validate(item):
            print(f"DataStream error - Can't process element in stream: {item}")
    ds.print_processors_stats()
    print("Registering other data processors")
    text_proc = TextProcessor()
    log_proc = LogProcessor()
    ds.register_processor(text_proc)
    ds.register_processor(log_proc)
    print("Send the same batch again")
    for item in batch:
        accepted = False
        for proc in [num_proc, text_proc, log_proc]:
            if proc.validate(item):
                proc.ingest(item)
                accepted = True
                break
        if not accepted:
            print(f"DataStream error - "
                  f"Can't process element in stream: {item}")
    ds.print_processors_stats()
    print("Consume some elements from the data processors: Numeric 3, "
          "Text 2, Log 1")
    for _ in range(3):
        if len(num_proc._storage) > 0:
            num_proc.output()
    for _ in range(2):
        if len(text_proc._storage) > 0:
            text_proc.output()
    for _ in range(1):
        if len(log_proc._storage) > 0:
            log_proc.output()
    ds.print_processors_stats()


if __name__ == "__main__":
    test_data_stream()
