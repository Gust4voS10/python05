from typing import Any, Union, List, Dict
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._storage: list[str] = []

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
                self._storage.append(str(item))
        else:
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
                self._storage.append(item)
        else:
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
        else:
            formatted = f"{data['log_level'].strip()}: {data['log_message']}"
            self._storage.append(formatted)


def test() -> None:
    print("Testing Numeric Processor...")
    np = NumericProcessor()

    print(f"Trying to validate input '42': {np.validate(42)}")
    print(f"Trying to validate input 'Hello': {np.validate('Hello')}")

    try:
        print("Test invalid ingestion of string "
              "'foo' without prior validation:")
        np.ingest("foo")  # type: ignore
    except Exception as e:
        print(f"Got exception: {e}")

    data = [1, 2, 3, 4, 5]
    print(f"Processing data: {data}")
    np.ingest(data)  # type: ignore

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

    print("\nTesting Log Processor...")
    lp = LogProcessor()

    print(f"Trying to validate input 'Hello': {lp.validate('Hello')}")

    log_data: list[dict[str, str]] = [
        {"log_level": "NOTICE", "log_message": "Connection to server"},
        {"log_level": "ERROR\n", "log_message": "Unauthorized access!!"},
    ]

    print(f"Processing data: {log_data}")
    lp.ingest(log_data)

    print("Extracting 2 values...")
    for i in range(2):
        print(f"Log entry {i}: {lp.output()}")


if __name__ == "__main__":
    test()
