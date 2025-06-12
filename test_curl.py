from pprint import pprint
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import app

load_dotenv()  # טוען את הקובץ .env

url = os.getenv("SLACK_FILE_URL")
api_token = os.getenv("api_token")
headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json'
}
res = requests.get(url, headers=headers)
#print(res.json())
csv_url = res.json()['list_csv_download_url']
# Download the CSV file
csv_res = requests.get(url=csv_url, headers=headers)
csv_res.raise_for_status()
csv_data = csv_res.content.decode('utf-8').splitlines()
total_csv = [dict(zip(csv_data[0].split(','), line.split(',')))
             for line in csv_data[1:]]
pprint(total_csv)  # צב
df = pd.DataFrame(res.json(), columns=app.slack_message_columns)
pprint(df)
slack_message_columns = [
            "id",
            "event_type",
            "user_id",
            "channel_id",
            "text",
            "ts",
            "parent_id",
            "is_list",
            "list_items",
            "num_list_items",
            "raw"
        ]
