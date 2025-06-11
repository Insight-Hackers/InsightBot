from pprint import pprint
import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()  # טוען את הקובץ .env

url = os.getenv("SLACK_FILE_URL")
api_token = os.getenv("api_token")
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}
res = requests.get(url, headers=headers)
csv_url = res.json()['list_csv_download_url']
# Download the CSV file
csv_res = requests.get(url=csv_url, headers=headers)
csv_res.raise_for_status()
csv_data = csv_res.content.decode('utf-8').splitlines()
total_csv = [dict(zip(csv_data[0].split(','), line.split(',')))
             for line in csv_data[1:]]
pprint(total_csv)  # צב

