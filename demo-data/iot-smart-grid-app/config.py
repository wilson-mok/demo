from dataclasses import dataclass

@dataclass
class Env:
    data_lake_name: str
    data_lake_access_key: str
    data_lake_container: str
    data_lake_folder: str

@dataclass
class Params:
    num_of_batches: int
    max_sleep_timer: int

@dataclass
class IotSmartGridConfig:
    env: Env
    params: Params