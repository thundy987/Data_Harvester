from abc import ABC, abstractmethod
from pathlib import Path


class SourceSystem(ABC):
    @property  # Pretend this attribute is a function
    @abstractmethod
    def source_location(self) -> str:
        pass

    @abstractmethod
    def fetch_data(self) -> tuple[list[Path], list[Path]]:
        pass

    @abstractmethod
    def extract_properties(self, path_to_file: Path | str) -> tuple:
        pass
