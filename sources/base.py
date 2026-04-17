from abc import ABC, abstractmethod
from pathlib import Path


class SourceSystem(ABC):
    @abstractmethod
    def fetch_data(self, scan_root_directory: str) -> tuple[list[Path], list[Path]]:
        pass

    @abstractmethod
    def extract_properties(self, path_to_file: Path | str) -> tuple:
        pass
