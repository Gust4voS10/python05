from typing import Any, Union, List, Dict
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    def __init__(self):
        self._storage: list[tuple[str]]

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._storage:
            raise IndexError("No data available in the processor.")
        return (self._storage.pop(0))


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        if (isinstance(data, list) and all(isinstance(i, (int, float)) for i in data)):
            return True
        return False

    def ingestdef(self, data:
                  Union[int, float, List[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise ValueError(f"Test invalid ingestion of string '{data}' "
                             f"without prior validation:")
        self._storage.append(str(data))


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list) and all(isinstance(i, str) for i in data):
            return True
        return False

    def ingestf(self, data: Union[str, List[str]]) -> None:
        if not self.validate(data):
            raise ValueError(f"Test invalid ingestion of string "
                             f"'{data}' without prior validation")
        self._storage.append(str(data))


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_log_dict(d):
            return (isinstance(d, dict)
                    and all(isinstance(k, str) and
                            isinstance(v, str) for k, v in d.items()))

        if is_log_dict(data):
            return True
        if isinstance(data, list) and all(is_log_dict(i) for i in data):
            return True
        return False

    def ingest(self, data:
               Union[Dict[str, str], List[Dict[str, str]]]) -> None:
        if not self.validate(data):
            raise ValueError(f"Dados inválidos para LogProcessor: {data}")
        self._storage.append(str(data))


def test() -> None:
    list__num = [1, 2, 3, 4, 5, 6.5]
    list_str = ["abc", "bcb", "cba"]
    list_log = {"abc": 2, "cbc": 3, "ada": 44}
