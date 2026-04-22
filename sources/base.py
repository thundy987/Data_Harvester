from abc import ABC, abstractmethod


class SourceSystem(ABC):
    @property
    @abstractmethod
    def source_location(self) -> str:
        pass

    @abstractmethod
    def fetch_data(self) -> dict[str, list[dict]]:
        pass


# {
#    'folders': [folder_list],
#    'files': [file_list],
#    'properties': [properties_list],
#    'references': [...],
# }
