import hydra
from hydra.core.config_store import ConfigStore
import asyncio
import random
from datetime import *
from dataclasses import dataclass, asdict, field
from dataclasses_json import dataclass_json, config
from marshmallow import fields
from config import *

# ConfigStore for Hydra
cs = ConfigStore.instance()
cs.store(name="iot_smart_grid_config", node=IotSmartGridConfig)

ZIP_CODE_RANGE = range(98001, 98101)
METER_ID_RANGE = range(1,5)
MEASUREMENT_MIN = 8.0
MEASUREMENT_MAX = 20.0

@dataclass_json
@dataclass
class SmartGridData: 
    meterId: str
    measurementInKWh: float
    zipCode: str
    measurementDate: str = field(
        default_factory=datetime.now,
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        ))


async def writeDataToADLS(jsonDataList) -> None:
    None

async def convertSmartGridDataToJSON(smartDataList) -> list:
    return [data.to_json() for data in smartDataList]
        

# Create the Smart Grid data
async def createSmartGridData(zipCodeList, meterIdRange, numOfMeterReading, sleepTimer) -> list:
    data = []

    await asyncio.sleep(sleepTimer)
    
    # Create smart grid data. For each zip code, we want an average of numOfMeterReading.
    for x in range(0, len(zipCodeList)*numOfMeterReading):
        selectMeterId = random.choice(meterIdRange)
        zipCode = random.choice(zipCodeList)

        meterId = f"{zipCode}-{selectMeterId}"
        measurementInKwh = random.uniform(MEASUREMENT_MIN, MEASUREMENT_MAX)

        data.append(SmartGridData(meterId=meterId, measurementInKWh=measurementInKwh, zipCode=zipCode))
    
    return data

# Create the data, convert it into JSON and write to Data Lake (parquet file format).
async def generateSmartGridData(zipCodeList, meterIdRange, numOfMeterReading, sleepTimer) -> None:
    result = await createSmartGridData(zipCodeList, meterIdRange, numOfMeterReading, sleepTimer)
    jsonItems = await convertSmartGridDataToJSON(result)
    print(jsonItems)
    await writeDataToADLS(jsonItems)

# Create the data, convert it into JSON and write to Data Lake (parquet file format).
async def asyncSmartGridData(zipCodeBatches, meterIdList, numOfMeterReading, maxSleepTimer) -> None:
    # ASync to Generate and process the data.
    await asyncio.gather(*[generateSmartGridData(b, meterIdList, numOfMeterReading, random.randint(1,maxSleepTimer)) for b in zipCodeBatches])

@hydra.main(version_base=None, config_path="config", config_name="config")
def main(cfg:IotSmartGridConfig) -> None: 
    zipCodeList = list(ZIP_CODE_RANGE)
    meterIdList = list(METER_ID_RANGE)

    zipCodeBatches = [zipCodeList[x:x+cfg.params.num_of_batches] for x in range(0, len(zipCodeList), cfg.params.num_of_batches)]

    asyncio.run(asyncSmartGridData(zipCodeBatches, meterIdList, cfg.params.num_of_meter_reading, cfg.params.max_sleep_timer))

if __name__ == "__main__":
    main()
    