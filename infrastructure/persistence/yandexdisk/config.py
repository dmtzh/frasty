from dataclasses import dataclass
from shared.utils.parse import NonEmptyStr, PositiveInt

@dataclass(frozen=True)
class YandexDiskRepositoryConfig:
    """
    Configuration for the Yandex Disk Repository.
    
    Attributes:
        base_folder: The root folder on Yandex Disk where items will be stored.
                     Must be a valid path (e.g., "/app_data/").
    """
    base_folder: NonEmptyStr
    oauth_token: NonEmptyStr
    timeout_seconds: PositiveInt
