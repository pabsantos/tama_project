import requests
import os
from dotenv import load_dotenv
from io import StringIO
import pandas as pd
import time
from datetime import datetime, timedelta


def get_datetime_range(start_date: str, end_date: str):
    datetime_format = "%Y%m%d%H%M"

    start = datetime.strptime(start_date, datetime_format)
    end = datetime.strptime(end_date, datetime_format)

    days = []
    while start <= end:
        days.append(start.strftime(datetime_format))
        start += timedelta(days=1)

    return days


def get_acum_historico(codibge: str, start_date: str, end_date: str, codestacao=None):
    url_base = "https://sws.cemaden.gov.br/PED/rest/"
    load_dotenv()
    token = os.getenv("API_TOKEN")
    group_url = url_base + "pcds-acum/acumulados-historicos"
    headers = {"token": token}

    df_list = []

    date_range = get_datetime_range(start_date, end_date)

    for date in date_range:
        params = {
            "codibge": codibge,
            "data": date,
            "formato": "CSV",
            "codestacao": codestacao,
        }
        response = requests.get(group_url, params=params, headers=headers)

        if response.status_code == 200:
            print(f"Request successful for {date}.")
        else:
            print(f"Error: code {response.status_code}; message {response.text}")
            raise Exception("Resquest not successful.")

        csv = response.text
        csv_buffer = StringIO(csv)
        df = pd.read_csv(csv_buffer)
        df["datetime"] = date
        df_list.append(df)
        time.sleep(5)

    return pd.concat(df_list, ignore_index=True)


def save_acum_historico(df: pd.DataFrame, start_date: str, end_date: str):
    path = "data/" + start_date + "_" + end_date + ".csv"
    df.to_csv(path, index=False)
