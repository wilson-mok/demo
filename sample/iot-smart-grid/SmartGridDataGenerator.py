import hydra
from hydra.core.config_store import ConfigStore
import asyncio
import uuid
import random
import json
import itertools
import logging
from datetime import *
from dataclasses import dataclass, field
from dataclasses_json import dataclass_json, config
from marshmallow import fields
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient, FileSystemClient
from config import *

# ConfigStore for Hydra
cs = ConfigStore.instance()
cs.store(name="iot_smart_grid_config", node=IotSmartGridConfig)

# Config logging
log = logging.getLogger(__file__)

# Meter Setup
ZIP_CODE_LIST = [98001,98002,98003,98004,98005,98006,98007,98008,98010,98011,98012,98014,98019,98020,98021,98022,98023,98024,98026,98027,98028,98029,98030,98031,98032,98033,98034,98036,98037,98038,98039,98040,98042,98043,98045,98047,98051,98052,98053,98055,98056,98057,98058,98059,98065,98070,98072,98074,98075,98077,98087,98092,98101,98102,98103,98104,98105,98106,98107,98108,98109,98110,98112,98115,98116,98117,98118,98119,98121,98122,98125,98126,98133,98134,98136,98144,98146,98148,98154,98155,98158,98164,98166,98168,98174,98177,98178,98188,98198,98199]
METER_ID_RANGE = range(1,5)
MEASUREMENT_MIN = 5.0
MEASUREMENT_MAX = 20.0

# Time setup
INITIAL_DATETIME = datetime.fromisoformat('2023-01-01T00:00:00')
MINUTE_INC = 15
NUM_TIME_PERIOD = 24 # Generate from midnight to 6am - 4 period/hr x 6 hrs.
NUM_READING = 3 # 5 min/reading

@dataclass_json
@dataclass
class SmartGridData: 
    meterId: str
    measurementInKWh: float
    zipCode: str
    measurementDate: datetime = field(
        default_factory=datetime.now,
        metadata=config(
            encoder=datetime.isoformat,
            decoder=datetime.fromisoformat,
            mm_field=fields.DateTime(format='iso')
        ))

# Write the Smart Grid data into the Data Lake
async def writeDataToADLS(smartDataList, fsClient, dataLakeFolder, sleepTimer) -> None:
    filename = f'{dataLakeFolder}/smartGridData-{str(uuid.uuid4())}.json'

    # Convert the smartDataList into a JSON format.
    jsonData = "\n".join([data.to_json() for data in smartDataList])
    jsonDataLen = len(jsonData)

    await asyncio.sleep(sleepTimer)
    log.info(f'Writing file - {filename}')

    try:
        fileClient = fsClient.create_file(file=filename)
        fileClient.append_data(jsonData, offset=0, length=jsonDataLen)
        fileClient.flush_data(jsonDataLen)

        log.info(f'Success - Writing file - {filename}')
    except Exception as ex:
        log.error(f'Failed - Writing file - {filename} - {ex}')

# Create the Smart Grid data
async def createSmartGridData(zipCodeList, meterIdList, startingDt) -> list:
    data = []

    zipCodeMeterList = list(itertools.product(zipCodeList, meterIdList))

    # Create smart grid data. For each zip code, we want numOfMeterReading.
    readingPeriod = int(MINUTE_INC/NUM_READING)

    for readingPoint in range(readingPeriod,MINUTE_INC+1,readingPeriod):
        readingDt = startingDt + timedelta(minutes=readingPoint)

        for zipCodeMeter in zipCodeMeterList:
            meterId = f"{zipCodeMeter[0]}-{zipCodeMeter[1]}"
            measurementInKwh = random.uniform(MEASUREMENT_MIN, MEASUREMENT_MAX)
            smData = SmartGridData(meterId=meterId, measurementInKWh=measurementInKwh, zipCode=zipCodeMeter[0], measurementDate=readingDt)
            data.append(smData)
    
    return data

# Generate the Smart Grid data for each time period and write to the data lake
async def generateSmartGridData(zipCodeList, meterIdRange, sleepTimer, fsClient, dataLakeFolder) -> None:

    for timeInc in range(0, NUM_TIME_PERIOD):
        dataDt = INITIAL_DATETIME + timedelta(minutes=(MINUTE_INC *timeInc))
        resultList = await createSmartGridData(zipCodeList, meterIdRange, dataDt)

        if len(resultList) > 0:
            log.info(f"Result generated for... {dataDt.isoformat()}")

            folderPath = dataDt.strftime("%Y/%m/%d/%H%M%S")
            await writeDataToADLS(resultList, fsClient, f"{dataLakeFolder}/{folderPath}", sleepTimer)
        else:
            log.info(f"No result generated for... {dataDt.isoformat()}")

# ASync to Generate and process the data.
async def asyncSmartGridData(zipCodeBatches, meterIdList, maxSleepTimer, fsClient, dataLakeFolder) -> None:
    await asyncio.gather(*[generateSmartGridData(b, meterIdList, random.randint(1,maxSleepTimer), fsClient, dataLakeFolder) for b in zipCodeBatches])


@hydra.main(config_path="config", config_name="config")
def main(cfg:IotSmartGridConfig) -> None: 
    meterIdList = list(METER_ID_RANGE)

    # divide the zip code into batches
    zipCodeBatches = [ZIP_CODE_LIST[x:x+cfg.params.num_of_batches] for x in range(0, len(ZIP_CODE_LIST), cfg.params.num_of_batches)]

    # Create a data lake connection
    if len(zipCodeBatches) > 0:
        connectionString = f'DefaultEndpointsProtocol=https;AccountName={cfg.env.data_lake_name};AccountKey={cfg.env.data_lake_access_key};EndpointSuffix=core.windows.net'
        adlsClient = DataLakeServiceClient.from_connection_string(conn_str=connectionString)
        fsClient = adlsClient.get_file_system_client(file_system=cfg.env.data_lake_container)

        log.info(f'Connecting to: {cfg.env.data_lake_name} - Container: {cfg.env.data_lake_container} - Data Lake folder: {cfg.env.data_lake_folder}')

        # Create and process the Smart Grid data - Async
        asyncio.run(asyncSmartGridData(zipCodeBatches, meterIdList, cfg.params.max_sleep_timer, fsClient, cfg.env.data_lake_folder))

if __name__ == "__main__":
    main()
    