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
PRIMARY_KEYS = {
    'slack_messages_raw': 'id',
    'alerts': 'id',
    'github_commits_raw': 'sha',
    'github_issues_raw': 'id',
    'github_prs_raw': 'id',
    'github_reviews_raw': 'id',
    'slack_reports_raw': 'id',
    # Composite key in DB, כאן עשוי להיות צורך בהתאמה מיוחדת
    'user_daily_summary': 'user_id',
}
df = pd.DataFrame([[
            res.json().get("client_msg_id") or res.json().get("ts"),
            "list",
            res.json().get("user"),
            res.json().get("channel"),
            total_csv,
            float(res.json().get("ts", 0)),
            res.json().get("parent_id")if res.json().get("parent_id") else None,
            True,
            total_csv,
            res.json()["files"][0]["list_limits"]["row_count"],
            res.json()
        ]], columns=slack_message_columns)

app.save_dataframe_to_db(df, "slack_messages_raw",PRIMARY_KEYS['slack_messages_raw'])
