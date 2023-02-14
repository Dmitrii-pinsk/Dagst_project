from minio import Minio
from dagster._core.definitions import solid, pipeline, hourly_schedule, repository
import requests
import csv
from datetime import datetime



@solid
def get_btc_quotes() -> None:
    print('start')
    url = "https://api.binance.com/api/v3/klines?symbol=BTCBUSD&interval=1h"
    response = requests.get(url)
    if response.status_code == 200:
        now_time = datetime.now()
        with open("file.csv", "w", newline="") as f:
            writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            writer.writerows(response.json())
        print('update csv')
        client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
        client.fput_object('pythonbucket', f"bucket/contents/{now_time.strftime('%d-%m-%Y %H-%M')}.csv", 'file.csv')
        print('done')


@pipeline
def testBtcPipeline():
    get_btc_quotes()


@hourly_schedule(pipeline_name="testBtcPipeline", start_date=datetime(2020, 2, 2))
def scheduleTest(date):
    return {
    }


@repository
def my_repository():
    return [
        testBtcPipeline,
        scheduleTest
    ]
