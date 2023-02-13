from dataclasses import dataclass

@dataclass
class Settings:
    blob_storage_account_url: str
    blob_storage_container: str
    blob_storage_access_key: str

@dataclass
class Params:
    num_of_batches: int
    num_of_meter_reading: int
    max_sleep_timer: int

@dataclass
class IotSmartGridConfig:
    settings: Settings
    params: Params