
from minio import Minio
from dagster._core.definitions import solid, pipeline, hourly_schedule, repository, asset
import requests
import csv
from datetime import datetime



@asset
def get_btc_quotes() -> None:
    print('done')
    url = "https://api.binance.com/api/v3/klines?symbol=BTCBUSD&interval=1h"
    response = requests.get(url)
    if response.status_code == 200:
        now_time = datetime.now()
        with open("file.csv", "w", newline="") as f:
            writer = csv.writer(f, delimiter=',', quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
            writer.writerows(response.json())
        print('done')
        client = Minio('localhost:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
        client.fput_object('pythonbucket', f"bucket/contents/{now_time.strftime('%d-%m-%Y %H-%M')}.csv", 'file.csv')